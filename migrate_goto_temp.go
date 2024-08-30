package migrate

import (
    "io"
    "os"
    "path/filepath"
    "strconv"
    "strings"

    "github.com/pkg/errors"
)

// Define a constant for the migration file name
const lastSuccessfulMigrationFile = "lastSuccessfulMigration"

func (m *Migrate) HandleDirtyState() error {
    // Perform actions when the database state is dirty
    lastSuccessfulMigrationPath := filepath.Join(m.ds.destPath, lastSuccessfulMigrationFile)
    lastVersionBytes, err := os.ReadFile(lastSuccessfulMigrationPath)
    if err != nil {
        return err
    }
    lastVersionStr := strings.TrimSpace(string(lastVersionBytes))
    lastVersion, err := strconv.ParseUint(lastVersionStr, 10, 64)
    if err != nil {
        return errors.Wrap(err, "failed to parse last successful migration version")
    }

    if err = m.Force(int(lastVersion)); err != nil {
        return errors.Wrap(err, "failed to apply last successful migration")
    }

    m.Log.Printf("Successfully applied migration: %s", lastVersionStr)

    if err = os.Remove(lastSuccessfulMigrationPath); err != nil {
        return err
    }

    m.Log.Printf("Successfully deleted file: %s", lastSuccessfulMigrationPath)
    return nil
}

func (m *Migrate) HandleMigrationFailure(curVersion int, v uint) error {
    failedVersion, _, err := m.databaseDrv.Version()
    if err != nil {
        return err
    }

    // Determine the last successful migration
    lastSuccessfulMigration := strconv.Itoa(curVersion)
    ret := make(chan interface{}, m.PrefetchMigrations)
    go m.read(curVersion, int(v), ret)

    for r := range ret {
        mig, ok := r.(*Migration)
        if ok {
            if mig.Version == uint(failedVersion) {
                break
            }
            lastSuccessfulMigration = strconv.Itoa(int(mig.Version))
        }
    }

    m.Log.Printf("migration failed, last successful migration version: %s", lastSuccessfulMigration)
    lastSuccessfulMigrationPath := filepath.Join(m.ds.destPath, lastSuccessfulMigrationFile)
    if err = os.WriteFile(lastSuccessfulMigrationPath, []byte(lastSuccessfulMigration), 0644); err != nil {
        return err
    }

    return nil
}

func (m *Migrate) CleanupFiles(v uint) error {
    if m.ds.destPath == "" {
        return nil
    }
    files, err := os.ReadDir(m.ds.destPath)
    if err != nil {
        return err
    }

    targetVersion := uint64(v)

    for _, file := range files {
        fileName := file.Name()

        // Check if file is a migration file we want to process
        if !strings.HasSuffix(fileName, "down.sql") && !strings.HasSuffix(fileName, "up.sql") {
            continue
        }

        // Extract version and compare
        versionEnd := strings.Index(fileName, "_")
        if versionEnd == -1 {
            // Skip files that don't match the expected naming pattern
            continue
        }

        fileVersion, err := strconv.ParseUint(fileName[:versionEnd], 10, 64)
        if err != nil {
            m.Log.Printf("Skipping file %s due to version parse error: %v", fileName, err)
            continue
        }

        // Delete file if version is greater than targetVersion
        if fileVersion > targetVersion {
            if err = os.Remove(filepath.Join(m.ds.destPath, fileName)); err != nil {
                m.Log.Printf("Failed to delete file %s: %v", fileName, err)
                continue
            }
            m.Log.Printf("Deleted file: %s", fileName)
        }
    }

    return nil
}

// CopyFiles copies all files from srcDir to destDir.
func (m *Migrate) CopyFiles() error {
    if m.ds.destPath == "" {
        return nil
    }
    _, err := os.ReadDir(m.ds.destPath)
    if err != nil {
        // If the directory does not exist
        return err
    }

    m.Log.Printf("Copying files from %s to %s", m.ds.srcPath, m.ds.destPath)

    return filepath.Walk(m.ds.srcPath, func(src string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        // ignore sub-directories in the migration directory
        if info.IsDir() {
            // Skip the tests directory and its files
            if info.Name() == "tests" {
                m.Log.Printf("Ignoring directory %s", info.Name())
                return filepath.SkipDir
            }
            return nil
        }
        // Ignore the current.sql file
        if info.Name() == "current.sql" {
            m.Log.Printf("Ignoring file %s", info.Name())
            return nil
        }

        var (
            srcFile  *os.File
            destFile *os.File
        )
        dest := filepath.Join(m.ds.destPath, info.Name())
        if srcFile, err = os.Open(src); err != nil {
            return err
        }
        defer func(srcFile *os.File) {
            if err = srcFile.Close(); err != nil {
                m.Log.Printf("failed to close file %s: %s", srcFile.Name, err)
            }
        }(srcFile)

        // Create the destination file
        if destFile, err = os.Create(dest); err != nil {
            return err
        }

        // Copy the file
        if _, err = io.Copy(destFile, srcFile); err == nil {
            return err
        }
        return os.Chmod(dest, info.Mode())
    })
}
