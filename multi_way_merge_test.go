package merged_fs

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"testing"
)

// This package benchmarks performance for accessing a many-way FS merge. It's
// intended as a benchmark; run it with "go test -bench=."

func openLargeZip(b *testing.B) []fs.FS {
	path := "test_data/many_zips.zip"
	f, e := os.Open(path)
	if e != nil {
		b.Logf("Failed opening %s: %s\n", path, e)
		b.FailNow()
	}
	stat, e := f.Stat()
	if e != nil {
		b.Logf("Failed to stat %s: %s\n", path, e)
		b.FailNow()
	}
	topFS, e := zip.NewReader(f, stat.Size())
	if e != nil {
		b.Logf("Failed getting zip reader for %s: %s\n", path, e)
		b.FailNow()
	}
	toReturn := make([]fs.FS, 2048)
	for i := 0; i < 2048; i++ {
		name := fmt.Sprintf("%d.zip", i+1)
		child, e := topFS.Open(name)
		if e != nil {
			b.Logf("Unable to open %s in %s: %s\n", name, path, e)
			b.FailNow()
		}
		// Wastes memory, but zip.NewReader needs an io.ReadSeeker, which the
		// normal zip FS doesn't provide.
		childContent, e := io.ReadAll(child)
		child.Close()
		if e != nil {
			b.Logf("Failed reading content of %s: %s\n", name, e)
			b.FailNow()
		}
		childReader := bytes.NewReader(childContent)
		childZip, e := zip.NewReader(childReader, int64(len(childContent)))
		if e != nil {
			b.Logf("Failed getting zip reader for %s: %s\n", name, e)
			b.FailNow()
		}
		toReturn[i] = childZip
	}
	f.Close()
	return toReturn
}

func BenchmarkLargeMerge(b *testing.B) {
	toMerge := openLargeZip(b)
	merged := MergeMultiple(toMerge...)

	// We'll benchmark the time required to open and close one of the random
	// .txt files in one of the 2048 merged zips.
	for n := 0; n < b.N; n++ {
		name := fmt.Sprintf("%d.txt", rand.Int31n(2048)+1)
		f, e := merged.Open(name)
		if e != nil {
			b.Logf("Failed opening %s: %s\n", name, e)
			b.FailNow()
		}
		stats, e := f.Stat()
		if e != nil {
			b.Logf("Failed getting stats for %s: %s\n", name, e)
			b.FailNow()
		}
		if stats.Size() < 1 {
			b.Logf("Expected %s to contain at least 1 byte of content\n", name)
			b.FailNow()
		}
		stats = nil
		f.Close()
	}
}
