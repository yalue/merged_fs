// The merged_fs library implements go1.16's filesystem interface (fs.FS) using
// two underlying FSs, presenting two (or more) filesystems as a single FS.
//
// Usage:
//
//    // fs1 and fs2 can be anything that supports the fs.FS interface,
//    // including other MergedFS instances.
//    fs1, _ := zip.NewReader(zipFile, fileSize)
//    fs2, _ := zip.NewReader(zipFile2, file2Size)
//    // Implements the io.FS interface, resolving conflicts in favor of fs1.
//    merged := NewMergedFS(fs1, fs2)
package merged_fs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"
)

// Implements the fs.FS interface, using the two underlying FS's. If a file is
// present in both filesystems, then the copy in A will always be preferred.
// This has an important implication: if a file is regular in A, but a
// directory in B, the entire directory in B will be ignored. If a file is a
// directory in both, then Open()-ing the file will result in a directory that
// contains the content from both FSs.
type MergedFS struct {
	// The two filesystems that have been merged. Do not modify these directly,
	// instead use NewMergedFS.
	A, B fs.FS

	// Used to speed up checks for whether a path in B is invalid due to it
	// including a directory with the name of a non-directory file in A.
	knownOKPrefixes map[string]bool
}

// Takes two FS instances and returns an initialized MergedFS.
func NewMergedFS(a, b fs.FS) *MergedFS {
	return &MergedFS{
		A:               a,
		B:               b,
		knownOKPrefixes: make(map[string]bool),
	}
}

// This is the key component of this library. It represents a directory that is
// present in both filesystems. Implements the fs.File, fs.DirEntry, and
// fs.FileInfo interfaces.
type MergedDirectory struct {
	// The path to this directory in both FSs
	name string
	// This will simply be the mode bits for FS A.
	mode fs.FileMode
	// This will be the most recent mod time (unix timestamp) from FS's A or
	// B.
	modTime uint64
	// The directory entries from both FS A and B, sorted alphabetically.
	entries []fs.DirEntry
	// The next entry to return with ReadDir.
	readOffset int
}

func (d *MergedDirectory) Name() string {
	return d.name
}

func (d *MergedDirectory) Mode() fs.FileMode {
	return d.mode
}

func (d *MergedDirectory) ModTime() time.Time {
	return time.Unix(int64(d.modTime), 0)
}

func (d *MergedDirectory) IsDir() bool {
	return true
}

func (d *MergedDirectory) Sys() interface{} {
	return nil
}

func (d *MergedDirectory) Stat() (fs.FileInfo, error) {
	return d, nil
}

func (d *MergedDirectory) Info() (fs.FileInfo, error) {
	return d, nil
}

func (d *MergedDirectory) Type() fs.FileMode {
	return d.mode.Type()
}

func (d *MergedDirectory) Size() int64 {
	return 0
}

func (d *MergedDirectory) Read(data []byte) (int, error) {
	return 0, fmt.Errorf("%s is a directory", d.name)
}

func (d *MergedDirectory) Close() error {
	// Note: Do *not* clear the rest of the fields here, since the
	// MergedDirectory also serves as a DirEntry or FileInfo field, which may
	// outlive the File itself being closed.
	d.entries = nil
	d.readOffset = 0
	return nil
}

func (d *MergedDirectory) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.readOffset >= len(d.entries) {
		if n <= 0 {
			// A special case required by the FS interface.
			return nil, nil
		}
		return nil, io.EOF
	}
	startEntry := d.readOffset
	var endEntry int
	if n <= 0 {
		endEntry = len(d.entries)
	} else {
		endEntry = startEntry + n
	}
	if endEntry > len(d.entries) {
		endEntry = len(d.entries)
	}
	toReturn := d.entries[startEntry:endEntry]
	d.readOffset = endEntry
	return toReturn, nil
}

// Returns the final element of the path. The path must be valid according to
// the rules of fs.ValidPath.
func baseName(path string) string {
	d := []byte(path)
	i := len(d)
	if i <= 1 {
		return path
	}
	i--
	for i >= 0 {
		if d[i] == '/' {
			break
		}
		i--
	}
	return string(d[i+1:])
}

// Returns a MergedDirectory, but doesn't set the entries slice or anything.
// (Intended to be used solely as a DirEntry, created with the same metadata
// as a MergedDirectory "File".
func getMergedDirEntry(a, b fs.DirEntry) (fs.DirEntry, error) {
	infoA, e := a.Info()
	if e != nil {
		return nil, fmt.Errorf("Failed getting info for file A: %w", e)
	}
	infoB, e := b.Info()
	if e != nil {
		return nil, fmt.Errorf("Failed getting info for file B: %w", e)
	}
	modTime := infoA.ModTime().Unix()
	modTimeB := infoB.ModTime().Unix()
	if modTimeB > modTime {
		modTime = modTimeB
	}
	return &MergedDirectory{
		name:       a.Name(),
		mode:       infoA.Mode(),
		modTime:    uint64(modTime),
		entries:    nil,
		readOffset: 0,
	}, nil
}

// Implements sort.Interface so we can sort entries by name.
type dirEntrySlice []fs.DirEntry

func (s dirEntrySlice) Len() int {
	return len(s)
}

func (s dirEntrySlice) Less(a, b int) bool {
	return s[a].Name() < s[b].Name()
}

func (s dirEntrySlice) Swap(a, b int) {
	s[a], s[b] = s[b], s[a]
}

// Takes two files that must be directories, and combines their contents into
// a single slice, sorted by name.
func mergeDirEntries(a, b fs.File) ([]fs.DirEntry, error) {
	dirA, ok := a.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("File A isn't a directory")
	}
	dirB, ok := b.(fs.ReadDirFile)
	if !ok {
		return nil, fmt.Errorf("File B isn't a directory")
	}
	entriesA, e := dirA.ReadDir(-1)
	if e != nil {
		return nil, fmt.Errorf("Failed reading entries from dir A: %w", e)
	}
	entriesB, e := dirB.ReadDir(-1)
	if e != nil {
		return nil, fmt.Errorf("Failed reading entries from dir B: %w", e)
	}

	// Maps the name to an existing index in toReturn.
	nameConflicts := make(map[string]int)
	toReturn := make([]fs.DirEntry, 0, len(entriesA)+len(entriesB))

	// Add the entries from directory A first.
	for _, entry := range entriesA {
		name := entry.Name()
		_, conflicts := nameConflicts[name]
		if conflicts {
			// Should never happen, as it would imply that FS A contained two
			// files with the same name in the same dir.
			return nil, fmt.Errorf("Duplicate name in dir A: %s", name)
		}
		nameConflicts[name] = len(toReturn)
		toReturn = append(toReturn, entry)
	}
	// Add the entries from directory B, skipping duplicate files, and updating
	// duplicate directory entries to share the same metadata. Otherwise, the
	// metadata returned by getting the info here may not match the metadata
	// returned by calling Open(..) on the dir. (required by testing/fstest)
	for _, entry := range entriesB {
		name := entry.Name()
		existingIndex, conflicts := nameConflicts[name]
		if !conflicts {
			// The name doesn't conflict, just add the entry and continue.
			nameConflicts[name] = len(toReturn)
			toReturn = append(toReturn, entry)
			continue
		}

		// The name conflicts, so look up the entry it conflicts with.
		existingEntry := toReturn[existingIndex]
		if !(existingEntry.IsDir() && entry.IsDir()) {
			// At least one of the conflicting entries isn't a directory so we
			// won't need to worry about a MergedDirectory's metadata not
			// matching.
			continue
		}

		// We have two conflicting directory entries, so we need to update the
		// DirEntry list we're returning to present metadata that matches the
		// data that will be returned by MergedDirectory.Stat().
		mergedDirEntry, e := getMergedDirEntry(existingEntry, entry)
		if e != nil {
			return nil, fmt.Errorf("Failed getting merged DirEntry for %s: %w",
				entry.Name(), e)
		}

		toReturn[existingIndex] = mergedDirEntry
	}

	// Finally, sort the results by name.
	sort.Sort(dirEntrySlice(toReturn))
	return toReturn, nil
}

// Creates and returns a new pseudo-directory "File" that contains the contents
// of both files a and b. Both a and b must be directories at the same
// specified path in m.A and m.B, respectively.
func (m *MergedFS) newMergedDirectory(a, b fs.File, path string) (fs.File,
	error) {
	sA, e := a.Stat()
	if e != nil {
		return nil, fmt.Errorf("Couldn't stat dir %s from FS A: %w", path, e)
	}
	sB, e := b.Stat()
	if e != nil {
		return nil, fmt.Errorf("Couldn't stat dir %s from FS B: %w", path, e)
	}
	modTime := sA.ModTime().Unix()
	modTimeB := sB.ModTime().Unix()
	if modTimeB > modTime {
		modTime = modTimeB
	}
	entries, e := mergeDirEntries(a, b)
	if e != nil {
		return nil, fmt.Errorf("Error merging directory contents: %w", e)
	}
	return &MergedDirectory{
		name:       baseName(path),
		mode:       sA.Mode(),
		modTime:    uint64(modTime),
		entries:    entries,
		readOffset: 0,
	}, nil
}

// Returns true if the given error is one that a filesystem may return when a
// path is invalid.
func isBadPathError(e error) bool {
	return errors.Is(e, fs.ErrNotExist) || errors.Is(e, fs.ErrInvalid)
}

// Returns an error if any prefix of the given path corresponds to a
// non-directory in m.A. Prefix components must therefore be either directories
// or nonexistent. Returns an error wrapping fs.ErrNotExist if any error is
// returned.
func (m *MergedFS) validatePathPrefix(path string) error {
	// Return immediately if we've already seen that this path is OK.
	if m.knownOKPrefixes[path] {
		return nil
	}
	components := strings.Split(path, "/")
	for i := range components {
		prefix := strings.Join(components[0:i+1], "/")
		if m.knownOKPrefixes[path] {
			return nil
		}
		f, e := m.A.Open(prefix)
		if e != nil {
			if isBadPathError(e) {
				// The path doesn't conflict--it doesn't exist in A.
				m.knownOKPrefixes[prefix] = true
				m.knownOKPrefixes[path] = true
				return nil
			}
			// We can't handle opening this path in A for some reason.
			return fmt.Errorf("%w: Error opening %s in A: %s", fs.ErrNotExist,
				path, e)
		}
		info, e := f.Stat()
		if e != nil {
			return fmt.Errorf("Couldn't stat file in A: %s", e)
		}
		if !info.IsDir() {
			// We found a non-dir file in A with the same name as the path.
			return fmt.Errorf("%w: %s is a file in A", fs.ErrNotExist,
				prefix)
		}
		// The prefix doesn't conflict (so far)--it is a directory in A.
		m.knownOKPrefixes[prefix] = true
	}
	// The path is a directory in A and B. (Though this should be unreachable
	// in the current usage of the function.)
	return nil
}

// If the path corresponds to a directory present in both A and B, this returns
// a MergedDirectory file. If it's present in both A and B, but isn't a
// directory in both, then this will simply return the copy in A. Otherwise,
// it returns the copy in B, so long as some prefix of the path doesn't
// correspond to a directory in A.
func (m *MergedFS) Open(path string) (fs.File, error) {
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{"open", "path", fs.ErrInvalid}
	}

	fA, e := m.A.Open(path)
	if e == nil {
		fileInfo, e := fA.Stat()
		if e != nil {
			return nil, fmt.Errorf("Couldn't stat %s in FS A: %w", path, e)
		}
		if !fileInfo.IsDir() {
			// If the file isn't a directory, we know it always overrides FS B,
			// so we don't even need to check FS B.
			return fA, nil
		}

		// The file is a directory in A, so we need to see if a directory with
		// the same name exists in B.
		fB, e := m.B.Open(path)
		if e != nil {
			if isBadPathError(e) {
				// The file doesn't exist in B, so return the copy in A.
				return fA, nil
			}
			// Treat any non-path errors in A or B as fatal.
			return nil, fmt.Errorf("Couldn't open %s in FS B: %w", path, e)
		}
		// Check if the file in B is a directory.
		fileInfo, e = fB.Stat()
		if e != nil {
			return nil, fmt.Errorf("Couldn't stat %s in FS B: %w", path, e)
		}
		if !fileInfo.IsDir() {
			// The file wasn't a dir in B, so ignore it in favor of the dir in
			// A.
			return fA, nil
		}
		// Finally, we know that the file is a directory in both A and B, so
		// return a MergedDirectory.
		return m.newMergedDirectory(fA, fB, path)
	}
	// Return an error now if the error isn't one we'd expect from an invalid
	// path.
	if !isBadPathError(e) {
		return nil, fmt.Errorf("Couldn't open %s in FS A: %w", path, e)
	}
	e = m.validatePathPrefix(path)
	if e != nil {
		// A file in A overrides a directory name in B, rendering this path
		// unreachable in the merged FS.
		return nil, &fs.PathError{"open", path, e}
	}
	// We already know the path doesn't exist in A, and isn't overshadowed by
	// a file named the same as a directory, so try to open it in B.
	return m.B.Open(path)
}
