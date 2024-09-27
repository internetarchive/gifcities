package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	encodedPath   = "/kubwa/gifcities/gifs_jsonl.gz" // aitio
	manifestPath  = "./data/gifcities-gifs.txt"
	htmlPath      = "./data/gifpages_html.jsonl.gz"
	jsonlPath     = "./data/gifcities.jsonl"
	mergedVecPath = "./data/gifcities_vec.jsonl"
	vecPath       = "./data/embeddings"
)

type Page struct {
	URL       string `json:"url"`
	Timestamp string `json:"timestamp"`
}

type Use struct {
	URL       string `json:"url"`
	Timestamp string `json:"timestamp"`
	Page      *Page  `json:"page"`
	Alt       string `json:"alt"`
	Path      string `json:"path"`
	Filename  string `json:"filename"`
}

type Vec struct {
	Vector []float64 `json:"vector"`
}

type Gif struct {
	Checksum string  `json:"checksum"`
	Terms    string  `json:"terms"`
	Uses     []Use   `json:"uses"`
	UseCount int     `json:"page_count"`
	Width    int32   `json:"width"`
	Height   int32   `json:"height"`
	Vecs     []Vec   `json:"vecs,omitempty"`
	MNSFW    float32 `json:"mnsfw"`
	KNSFW    bool    `json:"knsfw"`
}

func parsePage(p string) *Page {
	timestamp, url := splitWaybackURL(p)
	return &Page{
		Timestamp: timestamp,
		URL:       url,
	}
}

func splitWaybackURL(u string) (string, string) {
	split := strings.SplitN(u, "/", 2)
	return split[0], split[1]
}

func parseUse(fields []string) (Use, error) {
	timestamp, u := splitWaybackURL(fields[0])

	pu, err := url.Parse(u)
	if err != nil {
		return Use{}, fmt.Errorf("could not parse url '%s': %w", u, err)
	}

	sp := strings.Split(u, "/")
	filename := strings.Split(sp[len(sp)-1], ".")[0]
	pathText := strings.Join(strings.Split(
		strings.TrimSuffix(pu.Path, filename+".gif"),
		"/"), " ")

	var page *Page
	if fields[4] != "-/-" {
		page = parsePage(fields[4])
	}

	return Use{
		URL:       u,
		Timestamp: timestamp,
		Filename:  filename,
		Path:      strings.TrimSpace(pathText),
		Page:      page,
		Alt:       "", // TODO
	}, nil
}

func manifest(manifestPath string) error {
	f, err := os.Open(manifestPath)
	if err != nil {
		return err
	}
	defer f.Close()
	out, err := os.Create(jsonlPath)
	if err != nil {
		return err
	}
	defer out.Close()

	seen := map[string]Gif{}

	// 20031224055733/http://geocities.com/+estranged+/sam.gif MA2RY6GRLVEBI5AJ5EUGLQUEECB3GS3V 72 72 20091027012515/http://geocities.com/+estranged+/
	s := bufio.NewScanner(f)
	for s.Scan() {
		var gif Gif
		line := s.Text()
		fields := strings.Split(line, " ")
		checksum := fields[1]
		if _, ok := seen[checksum]; ok {
			gif = seen[checksum]
		} else {
			width, err := strconv.ParseInt(fields[2], 10, 32)
			if err != nil {
				return fmt.Errorf("bad width '%s': %w", fields[2], err)
			}
			height, err := strconv.ParseInt(fields[3], 10, 32)
			if err != nil {
				return fmt.Errorf("bad height '%s': %w", fields[3], err)
			}
			gif = Gif{
				Checksum: checksum,
				Uses:     []Use{},
				Width:    int32(width),
				Height:   int32(height),
			}
		}

		gif.UseCount++

		use, err := parseUse(fields)
		if err != nil {
			return err
		}
		gif.Uses = append(gif.Uses, use)

		seen[checksum] = gif
	}
	err = s.Err()
	if err != nil {
		return err
	}

	if err = s.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	for _, gif := range seen {
		bs, err := json.Marshal(gif)
		if err != nil {
			return fmt.Errorf("failed to serialize %s: %w", gif.Checksum, err)
		}

		fmt.Fprintf(out, "%s\n", strings.ReplaceAll(string(bs), "\n", ""))
	}

	return nil
}

func alt(htmlPath string) error {
	entries, err := os.ReadDir(htmlPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".jsonl.gz") {
			continue
		}
		f, err := os.Open(path.Join(htmlPath, entry.Name()))
		if err != nil {
			return err
		}
		zr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		s := bufio.NewScanner(zr)
		if err != nil {
			return err
		}
		buf := make([]byte, 0, 24*1024*1024)
		s.Buffer(buf, 24*1024*1024)

		for s.Scan() {
			line := s.Text()
			p := htmlpayload{}
			err := json.Unmarshal([]byte(line), &p)
			if err != nil {
				return err
			}
			imgs := findImgs(p.HTML)
			p.Imgs = imgs
			p.HTML = ""
			out, err := json.Marshal(p)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "%s\n", strings.ReplaceAll(string(out), "\n", ""))
		}
		if err = s.Err(); err != nil {
			return err
		}
	}

	return nil
}

type img struct {
	Src string
	Alt string
}

type htmlpayload struct {
	URL       string
	Timestamp string
	HTML      string `json:"html,omitempty"`
	Imgs      []img
}

func findImgs(s string) []img {
	out := []img{}
	imgTagRe := regexp.MustCompile(`^\s*?img`)
	tagBuff := ""
	inTag := false
	end := len(s)
	pos := 0
	for true {
		if pos >= end {
			break
		}

		if inTag {
			tagBuff = tagBuff + string(s[pos])
		}

		if s[pos] == '<' {
			inTag = true
		}

		if s[pos] == '>' {
			inTag = false
			lowered := strings.ToLower(tagBuff)
			//if strings.Contains(lowered, "img") {
			//	fmt.Printf("TAGBUFF: '%s'\n", tagBuff)
			//	fmt.Printf("%v\n", imgTagRe.MatchString(lowered))
			//	fmt.Printf("%v\n", strings.Contains(lowered, ".gif"))
			//	fmt.Printf("%v\n", strings.Contains(lowered, "alt"))
			//}
			if imgTagRe.MatchString(lowered) && strings.Contains(lowered, ".gif") && strings.Contains(lowered, "alt") {
				//fmt.Println("YEEHAW")
				alt := extractProp(tagBuff, "alt")
				src := extractProp(tagBuff, "src")
				//fmt.Printf("alt '%s' src '%s'\n", alt, src)
				if alt != "" || src != "" {
					out = append(out, img{Src: src, Alt: alt})
				}
			}
			tagBuff = ""
		}

		pos++
	}
	return out
}

var propExtractSingRe = regexp.MustCompile(`'([^']+)'`)
var propExtractDoubRe = regexp.MustCompile(`"([^"]+)"`)

func extractProp(s, prop string) string {
	propIx := strings.Index(s, prop)
	if propIx < 0 {
		return ""
	}
	start := propIx + len(prop)
	if start > len(s)-1 {
		return ""
	}
	var re *regexp.Regexp
	for x := start; x < len(s); x++ {
		if string(s[x]) == `"` {
			re = propExtractDoubRe
			break
		}
		if string(s[x]) == "'" {
			re = propExtractSingRe
			break
		}
	}
	if re == nil {
		return ""
	}
	matches := re.FindStringSubmatch(s[start:])
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func eximg() error {
	f, err := os.Open("gifs_jsonl-00000")
	if err != nil {
		return err
	}
	r := bufio.NewScanner(f)
	x := 0
	for r.Scan() {
		if x == 1 {
			break
		}
		x++
		l := r.Text()
		type p struct {
			Gifb64 string
		}
		pp := p{}
		err := json.Unmarshal([]byte(l), &pp)
		if err != nil {
			return fmt.Errorf("could not unmarshal: %w", err)
		}

		nf, err := os.Create("img.gif")
		if err != nil {
			return err
		}
		defer nf.Close()
		bs, err := base64.StdEncoding.DecodeString(pp.Gifb64)
		if err != nil {
			return err
		}

		nf.Write(bs)
	}
	return nil
}

func upload(encodedPath string) error {
	// This code is only intended to be run from aitio
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey == "" {
		return errors.New("need S3_ACCESS_KEY in env")
	}
	if accessKey == "" {
		return errors.New("need S3_SECRET_KEY in env")
	}
	lFile, err := os.Create("hashes.log")
	if err != nil {
		return err
	}

	hashLog := log.New(lFile, "", log.Lshortfile)
	defer lFile.Close()
	ctx := context.Background()
	bucket := "gifcities"
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		return errors.New("need S3_ENDPOINT in env")
	}
	s3c, err := minio.New(endpoint,
		&minio.Options{
			// Note: seaweedfs (version 8000GB 1.79 linux amd64) may not work
			// with V4!
			Creds:  credentials.NewStaticV2(accessKey, secretKey, ""),
			Secure: false,
		},
	)
	if err != nil {
		return err
	}

	ok, err := s3c.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("bucket exist failed: %w", err)
	}
	if !ok {
		opts := minio.MakeBucketOptions{}
		if err := s3c.MakeBucket(ctx, bucket, opts); err != nil {
			return fmt.Errorf("make bucket failed: %w", err)
		}
	}

	entries, err := os.ReadDir(encodedPath)
	if err != nil {
		return fmt.Errorf("could not read jsonl dir '%s': %w", encodedPath, err)
	}

	total := 0.0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			total++
		}
	}

	putOpts := minio.PutObjectOptions{
		ContentType: "image/gif",
	}
	statOpts := minio.StatObjectOptions{}

	uploads := 0
	prevSeen := 0
	readLines := 0

	for x, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".gz") {
			continue
		}
		f, err := os.Open(path.Join(encodedPath, entry.Name()))
		if err != nil {
			return err
		}
		defer f.Close()

		zr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}

		s := bufio.NewScanner(zr)

		type encodedGif struct {
			Hash   string
			Gifb64 string
		}

		buf := make([]byte, 0, 24*1024*1024)
		s.Buffer(buf, 24*1024*1024)

		for s.Scan() {
			line := s.Text()
			readLines++
			fmt.Printf("\033[2K\r%d/%f .gz files | %d lines read | %d prevSeen | %d uploads",
				x+1, total, readLines, prevSeen, uploads)
			p := encodedGif{}
			err := json.Unmarshal([]byte(line), &p)
			if err != nil {
				return err
			}
			_, err = s3c.StatObject(ctx, bucket, p.Hash, statOpts)
			if err == nil {
				prevSeen++
				hashLog.Printf("skip\t%s\n", p.Hash)
				// TODO could check size of object and see if it matches
				continue
			}
			bs, err := base64.StdEncoding.DecodeString(p.Gifb64)
			if err != nil {
				return err
			}
			info, err := s3c.PutObject(ctx, bucket, p.Hash, bytes.NewReader(bs), int64(len(bs)), putOpts)
			if err != nil {
				hashLog.Printf("fail\t%s\n", p.Hash)
				return fmt.Errorf("put object failed for '%s': %w", p.Hash, err)
			}
			hashLog.Printf("success\t%s\n", p.Hash)
			uploads++
			// TODO how likely are these cases?
			if info.Bucket != bucket {
				return fmt.Errorf("[put] bucket mismatch: %v", info.Bucket)
			}
			if info.Key != p.Hash {
				return fmt.Errorf("[put] key mismatch: %v", info.Key)
			}
		}
		err = s.Err()
		if err != nil {
			return err
		}
	}

	fmt.Println()
	fmt.Println()
	fmt.Printf("ignored %d seen hashes\n", prevSeen)
	fmt.Printf("uploaded %d gifs\n", uploads)

	return nil
}

func gzScanner(gzpath string) (s *bufio.Scanner, err error) {
	vf, err := os.Open(gzpath)
	if err != nil {
		return nil, fmt.Errorf("failed to open '%s': %w", gzpath, err)
	}

	zr, err := gzip.NewReader(vf)
	if err != nil {
		return
	}
	s = bufio.NewScanner(zr)
	buf := make([]byte, 0, 24*1024*1024)
	s.Buffer(buf, 24*1024*1024)
	return
}

func vecmerge(vp string) error {
	gifs := map[string]*Gif{}
	f, err := os.Open(jsonlPath)
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	buf := make([]byte, 0, 24*1024*1024)
	s.Buffer(buf, 24*1024*1024)
	for s.Scan() {
		gif := Gif{}
		if err := json.Unmarshal(s.Bytes(), &gif); err != nil {
			return err
		}
		gifs[gif.Checksum] = &gif
	}
	if s.Err() != nil {
		return fmt.Errorf("gifcities.jsonl scanner failed: %w", s.Err())
	}

	// TODO readdir
	entries, err := os.ReadDir(vp)
	if err != nil {
		return fmt.Errorf("could not read jsonl dir '%s': %w", encodedPath, err)
	}

	out, err := os.Create(mergedVecPath)
	if err != nil {
		return err
	}
	defer out.Close()

	type VecLine struct {
		Hash  string
		MNSFW float32 `json:"mnsfw"`
		// TODO waiting on KNSFW
		Embedding []float64
	}

	for _, e := range entries {
		vf, err := os.Open(e.Name())
		if err != nil {
			return err
		}
		defer vf.Close()
		zr, err := gzip.NewReader(vf)
		if err != nil {
			return err
		}
		s = bufio.NewScanner(zr)
		if err != nil {
			return err
		}
		s.Buffer(buf, 24*1024*1024)
		for s.Scan() {
			vl := VecLine{}
			if err := json.Unmarshal(s.Bytes(), &vl); err != nil {
				return fmt.Errorf("failed to deserialize embedding: %w", err)
			}
			g, ok := gifs[vl.Hash]
			if !ok {
				fmt.Fprintf(os.Stderr, "WARN checksum '%s' not found in gifcities.jsonl\n", vl.Hash)
				continue
			}
			if g.Vecs == nil {
				g.Vecs = []Vec{}
			}
			g.Vecs = append(g.Vecs, Vec{Vector: vl.Embedding})
			g.MNSFW = vl.MNSFW
		}
		if s.Err() != nil {
			return fmt.Errorf("embeddings scanner failed: %w", s.Err())
		}
	}

	for _, g := range gifs {
		bs, err := json.Marshal(g)
		if err != nil {
			return fmt.Errorf("failed to serialize %s: %w", g.Checksum, err)
		}

		fmt.Fprintf(out, "%s\n", strings.ReplaceAll(string(bs), "\n", ""))
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "need a subcommand")
		os.Exit(2)
	}

	var err error

	switch os.Args[1] {
	case "upload":
		err = upload(encodedPath)
	case "manifest":
		mp := manifestPath
		if len(os.Args) == 3 {
			mp = os.Args[2]
		}
		err = manifest(mp)
	case "alt":
		err = alt(htmlPath)
	case "eximg":
		err = eximg()
	case "vecmerge":
		vp := vecPath
		if len(os.Args) == 3 {
			vp = os.Args[2]
		}
		err = vecmerge(vp)
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand")
		os.Exit(3)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed: %s\n", err.Error())
		os.Exit(1)
	}
}
