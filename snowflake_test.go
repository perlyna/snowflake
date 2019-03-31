package snowflake

import (
	"testing"
)

func Test_Next(t *testing.T) {
	worker := NewWorker(0, 0)
	_, err := worker.Next()
	if err != nil {
		t.Error(err)
	}

}
