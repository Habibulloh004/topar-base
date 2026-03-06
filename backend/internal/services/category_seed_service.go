package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
)

type CategorySeedResult struct {
	AlreadySeeded bool
	InsertedCount int
	FilePath      string
}

func SeedCategoriesIfEmpty(ctx context.Context, repo *repository.CategoryRepository, configPath string) (CategorySeedResult, error) {
	result := CategorySeedResult{}
	if repo == nil {
		return result, errors.New("category repository is nil")
	}

	count, err := repo.Count(ctx)
	if err != nil {
		return result, err
	}
	if count > 0 {
		result.AlreadySeeded = true
		return result, nil
	}

	categories, usedPath, err := loadSeedCategories(configPath)
	if err != nil {
		return result, err
	}

	inserted, err := repo.ReplaceFromInputs(ctx, categories)
	if err != nil {
		return result, err
	}

	result.InsertedCount = inserted
	result.FilePath = usedPath
	return result, nil
}

func StartCategorySeedInBackground(
	parentCtx context.Context,
	timeout time.Duration,
	repo *repository.CategoryRepository,
	configPath string,
) {
	go func() {
		if timeout <= 0 {
			timeout = 45 * time.Second
		}

		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		defer cancel()

		result, err := SeedCategoriesIfEmpty(ctx, repo, configPath)
		if err != nil {
			log.Printf("main_categories startup seed failed in background: %v", err)
			return
		}

		if result.AlreadySeeded {
			log.Printf("main_categories startup seed skipped: already filled")
			return
		}

		log.Printf("main_categories auto-seeded in background: inserted=%d file=%s", result.InsertedCount, result.FilePath)
	}()
}

func loadSeedCategories(configPath string) ([]models.CategoryInput, string, error) {
	paths := []string{}
	if configPath != "" {
		paths = append(paths, configPath)
	}

	paths = append(paths,
		"chitai-gorod-categories.json",
		filepath.Join("..", "chitai-gorod-categories.json"),
		"/app/chitai-gorod-categories.json",
	)

	if executablePath, err := os.Executable(); err == nil {
		execDir := filepath.Dir(executablePath)
		paths = append(paths,
			filepath.Join(execDir, "chitai-gorod-categories.json"),
			filepath.Join(execDir, "..", "chitai-gorod-categories.json"),
		)
	}

	seen := map[string]struct{}{}
	var usedPath string
	var data []byte
	for _, path := range paths {
		clean := filepath.Clean(path)
		if _, exists := seen[clean]; exists {
			continue
		}
		seen[clean] = struct{}{}

		bytes, err := os.ReadFile(clean)
		if err != nil {
			continue
		}
		data = bytes
		usedPath = clean
		break
	}

	if len(data) == 0 {
		return nil, "", fmt.Errorf("could not find chitai-gorod-categories.json; set CATEGORIES_FILE to explicit path")
	}

	var categories []models.CategoryInput
	if err := json.Unmarshal(data, &categories); err != nil {
		return nil, "", err
	}
	if len(categories) == 0 {
		return nil, "", errors.New("categories file is empty")
	}

	return categories, usedPath, nil
}
