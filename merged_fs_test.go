package merged_fs

import (
	"archive/zip"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
)

func openZip(path string, t *testing.T) fs.FS {
	// We don't bother to close these .zip archives used for testing.
	f, e := os.Open(path)
	if e != nil {
		t.Logf("Failed opening %s: %s\n", path, e)
		t.FailNow()
	}
	stat, e := f.Stat()
	if e != nil {
		t.Logf("Failed to stat file %s: %s\n", path, e)
		t.FailNow()
	}
	toReturn, e := zip.NewReader(f, stat.Size())
	if e != nil {
		t.Logf("Failed to open zip file %s: %s\n", path, e)
		t.FailNow()
	}
	return toReturn
}

func TestMergedFS(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)

	// TODO (next): Fix failure here. It merges just two zip files without
	// failures, but merging zip1 with a NewMergedFS will fail.

	// Merge all three zip files into a single FS, with zip1 being highest
	// priority, and zip3 being lowest priority.
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))
	expectedFiles := []string{
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
	}

	e := fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("TestFS failed: %s\n", e)
		t.FailNow()
	}

	// TODO:
	//  - Make sure that a/test4.txt is *not* available (since a should be a
	//    file, not a directory.
	//  - Make sure that when opening a, IsDir() is false.
	//  - Make sure that test1.txt is the copy from test1.zip, with a size of
	//    2 bytes.
}
