package merged_fs

import (
	"archive/zip"
	"io"
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

	// Merge all three zip files into a single FS, with zip1 being highest
	// priority, and zip3 being lowest priority.
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))
	expectedFiles := []string{
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
		"b",
		"a",
	}

	e := fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("TestFS failed: %s\n", e)
		t.FailNow()
	}

	// Make sure we can't treat a as both a regular file and a directory.
	_, e = merged.Open("a/test4.txt")
	if e == nil {
		t.Logf("Didn't get expected error when opening a/test4.txt. It " +
			"shouldn't be available, since a is a regular file.\n")
		t.FailNow()
	}
	t.Logf("Got expected error when opening a file in an overridden "+
		"directory: %s", e)
	f, e := merged.Open("a")
	if e != nil {
		t.Logf("Failed to open file a: %s\n", e)
		t.FailNow()
	}
	stat, e := f.Stat()
	if e != nil {
		t.Logf("Failed to Stat() file a: %s\n", e)
		t.FailNow()
	}
	if stat.IsDir() {
		t.Logf("Expected file a to be a regular file, not a directory.\n")
		t.Fail()
	}

	// Make sure this is the copy from test_a.zip, which should be two bytes.
	f, e = merged.Open("test1.txt")
	if e != nil {
		t.Logf("Failed opening test1.txt: %s\n", e)
		t.FailNow()
	}
	stat, e = f.Stat()
	if e != nil {
		t.Logf("Failed to Stat() test1.txt: %s\n", e)
		t.FailNow()
	}
	if stat.Size() != 2 {
		t.Logf("Expected test1.txt to be 2 bytes, got %d.\n", stat.Size())
		t.Fail()
	}
	content, e := io.ReadAll(f)
	if e != nil {
		t.Logf("Failed reading test1.txt's content: %s", e)
		t.FailNow()
	}
	t.Logf("Content of test1.txt: %s\n", string(content))
}

func TestDataRace(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))

	// We'll use the same files as in the previous test, but here we'll try to
	// open them all concurrently.
	expectedFiles := []string{
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
		"b",
		"a",
	}

	for _, filename := range expectedFiles {
		go func(name string) {
			f, e := merged.Open(name)
			if e != nil {
				t.Logf("Failed opening %s: %s\n", name, e)
				t.Fail()
			}
			f.Close()
		}(filename)
	}
}
