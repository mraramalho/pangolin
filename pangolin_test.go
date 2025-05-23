package pangolin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var p = New()

func TestNrows(t *testing.T) {
	// Exemplo de uso
	p, err := p.ReadExcel("exemplo.xlsx", "Plan1", false)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 34, p.Nrows())
}
