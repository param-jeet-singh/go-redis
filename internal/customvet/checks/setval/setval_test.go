package setval_test

import (
	"testing"

	"github.com/param-jeet-singh/go-redis/internal/customvet/checks/setval"
	"golang.org/x/tools/go/analysis/analysistest"
)

func Test(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, setval.Analyzer, "a")
}
