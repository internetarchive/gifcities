package main

import "testing"

func Test_extractProp(t *testing.T) {
	type kase struct {
		name   string
		prop   string
		s      string
		expect string
	}

	cases := []kase{
		{
			name:   "no such prop",
			prop:   "foo",
			s:      "hi='bye'",
			expect: "",
		},
		{
			name:   "easy",
			prop:   "alt",
			s:      `alt="great cool hey"`,
			expect: "great cool hey",
		},
		{
			name:   "multiple",
			prop:   "alt",
			s:      `bar="llolo lo lol'" alt =" great ok hi" src= "nobody" foo='bar'`,
			expect: " great ok hi",
		},
		{
			name:   "quotey",
			prop:   "src",
			s:      `bar='cool' alt = "hi there how" src   =    "fabulous ' muscles"`,
			expect: "fabulous ' muscles",
		},
		{
			name:   "nested single in double quote",
			prop:   "src",
			s:      `bar='cool' alt = "hi there how" src   =    "fabulous ' muscles"`,
			expect: "fabulous ' muscles",
		},
		{
			name:   "nested double in single quote",
			prop:   "src",
			s:      `bar='cool' alt = "hi there how" src   =    'fabulous " muscles'`,
			expect: `fabulous " muscles`,
		},
		{
			name:   "unclosed to end",
			prop:   "alt",
			s:      `src="wheee.jpg" alt="this is my alt oops`,
			expect: ``,
		},
		{
			name:   "unclosed to other prop",
			prop:   "alt",
			s:      `src="wheee.jpg" alt="this is my alt oops foo='bar' baz="quux"`,
			expect: `this is my alt oops foo='bar' baz=`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := extractProp(c.s, c.prop)
			if result != c.expect {
				t.Errorf("expected '%s', got '%s'", c.expect, result)
			}
		})
	}
}
