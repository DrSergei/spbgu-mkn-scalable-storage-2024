package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	containsZero := false
	containsOne := false
	for i := 0; i < len(b); i++ {
		if b[i] == '0' {
			containsZero = true
		} else if b[i] == '1' {
			containsOne = true
		} else {
			t.Errorf("Unexpected byte: %b", b[i])
		}
	}
	assert.True(t, containsZero, "Expected '0' in b")
	assert.True(t, containsOne, "Expected '1' in b")
}
