package domain

import (
	"errors"
	"testing"
)

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to CustomerStatus
		want     bool
	}{
		{StatusNew, StatusProfileFilled, true},
		{StatusProfileFilled, StatusOnKyc, true},
		{StatusOnKyc, StatusActive, true},
		{StatusOnKyc, StatusRejected, true},
		{StatusNew, StatusActive, false}, // перескок запрещён
		{StatusActive, StatusProfileFilled, false},
		{StatusRejected, StatusNew, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("%s → %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestUpdateProfile_LockedAfterKyc(t *testing.T) {
	c := &Customer{Status: StatusOnKyc}
	err := c.UpdateProfile("a", "b", "c", "2000-01-01", "d", "e")
	if !errors.Is(err, ErrProfileLocked) {
		t.Fatalf("ожидали ErrProfileLocked, получили: %v", err)
	}
}

func TestUpdateProfile_FillsFromNew(t *testing.T) {
	c := &Customer{Status: StatusNew}
	if err := c.UpdateProfile("Ivan", "Ivanov", "+995...", "1990-05-01", "Tbilisi", "GE"); err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if c.Status != StatusProfileFilled {
		t.Errorf("статус = %s, ожидали PROFILE_FILLED", c.Status)
	}
}
