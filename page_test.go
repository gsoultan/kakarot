package kakarot_test

import (
	"testing"

	"github.com/gsoultan/kakarot"
)

type pos struct {
	N int `json:"n"`
}

func TestPagination(t *testing.T) {
	rows := []int{1, 2, 3}
	p, err := kakarot.NextPage(rows, 2, func(r int) int { return r * 10 }, func(r int) pos { return pos{N: r} })
	if err != nil || len(p.Items) != 2 || p.Next == "" {
		t.Fatalf("%+v %v", p, err)
	}
	c, ok, err := kakarot.DecodeCursor[pos](p.Next)
	if err != nil || !ok || c.N != 2 {
		t.Fatalf("%+v %v %v", c, ok, err)
	}
	last, _ := kakarot.NextPage(rows[:1], 2, func(r int) int { return r }, func(r int) pos { return pos{N: r} })
	if last.Next != "" {
		t.Fatal("last page has a cursor")
	}
	if _, _, err := kakarot.DecodeCursor[pos]("!!"); kakarot.CodeOf(err) != kakarot.CodeInvalid {
		t.Fatal(err)
	}
	if (kakarot.PageRequest{Limit: 500}).Size(20, 100) != 100 {
		t.Fatal("clamp")
	}
}
