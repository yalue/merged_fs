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

// test_data/many_zips.zip contains 2,048 zip files, each of which contains a
// single .txt file. This function returns a slice of filesystems: one per zip
// file in many_zips.zip.
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

func TestEmptyMergeMultiple(t *testing.T) {
	merged := MergeMultiple()
	if merged == nil {
		t.Logf("Didn't get a valid FS from MergeMultiple with no args.\n")
		t.FailNow()
	}
	f, e := merged.Open("./bad.txt")
	if e == nil {
		t.Logf("Didn't get expected error when opening a file in empty FS.\n")
		t.FailNow()
	}
	if !isBadPathError(e) {
		t.Logf("Didn't get bad path error when opening a file in empty FS. "+
			"Got %s instead.\n", e)
		t.FailNow()
	}
	t.Logf("Got expected error from opening a file in the empty FS: %s\n", e)
	f, e = merged.Open(".")
	if e != nil {
		t.Logf("Error opening \".\" in empty FS: %s\n", e)
		t.FailNow()
	}
	info, e := f.Stat()
	if e != nil {
		t.Logf("Error getting info for \".\" in empty FS: %s\n", e)
		t.FailNow()
	}
	e = f.Close()
	if e != nil {
		t.Logf("Error closing \".\" in empty FS: %s\n", e)
		t.FailNow()
	}
	if !info.IsDir() {
		t.Logf("\".\" in empty FS wasn't marked as a directory.\n")
		t.FailNow()
	}
}
