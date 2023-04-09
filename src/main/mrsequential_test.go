package main

import (
	"reflect"
	"testing"

	"6.824/mr"
)

func Test_loadPlugin(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name  string
		args  args
		want  func(string, string) []mr.KeyValue
		want1 func(string, []string) string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := loadPlugin(tt.args.filename)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadPlugin() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("loadPlugin() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
