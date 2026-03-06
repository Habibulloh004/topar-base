package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
)

type EksmoBootstrapResult struct {
	AlreadyFilled bool
	ProductsCount int64
	SubjectsCount int64
	NichesCount   int64
	SyncResult    models.EksmoSyncResult
}

func BootstrapEksmoCoreCollectionsIfEmpty(
	ctx context.Context,
	service *EksmoService,
	productRepo *repository.EksmoProductRepository,
	subjectRepo *repository.EksmoSubjectRepository,
	nicheRepo *repository.EksmoNicheRepository,
) (EksmoBootstrapResult, error) {
	result := EksmoBootstrapResult{}

	if service == nil {
		return result, fmt.Errorf("eksmo service is nil")
	}
	if productRepo == nil || subjectRepo == nil || nicheRepo == nil {
		return result, fmt.Errorf("eksmo repositories are not fully configured")
	}

	productsCount, err := productRepo.Count(ctx)
	if err != nil {
		return result, err
	}
	subjectsCount, err := subjectRepo.Count(ctx)
	if err != nil {
		return result, err
	}
	nichesCount, err := nicheRepo.Count(ctx)
	if err != nil {
		return result, err
	}

	result.ProductsCount = productsCount
	result.SubjectsCount = subjectsCount
	result.NichesCount = nichesCount

	if productsCount > 0 && subjectsCount > 0 && nichesCount > 0 {
		result.AlreadyFilled = true
		return result, nil
	}

	if err := subjectRepo.EnsureIndexes(ctx); err != nil {
		return result, err
	}
	if err := nicheRepo.EnsureIndexes(ctx); err != nil {
		return result, err
	}

	syncResult, err := service.SyncAllProductsWithExtraction(ctx, ProductSyncRepos{
		Products: productRepo,
		Subjects: subjectRepo,
		Niches:   nicheRepo,
	}, EksmoSyncOptions{
		PerPage:  0,
		MaxPages: 0,
		Resume:   false,
		Reset:    true,
	})
	if err != nil {
		return result, err
	}

	result.SyncResult = syncResult
	return result, nil
}

func StartEksmoBootstrapInBackground(
	parentCtx context.Context,
	timeout time.Duration,
	service *EksmoService,
	productRepo *repository.EksmoProductRepository,
	subjectRepo *repository.EksmoSubjectRepository,
	nicheRepo *repository.EksmoNicheRepository,
) {
	go func() {
		if timeout <= 0 {
			timeout = 24 * time.Hour
		}

		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		defer cancel()

		result, err := BootstrapEksmoCoreCollectionsIfEmpty(ctx, service, productRepo, subjectRepo, nicheRepo)
		if err != nil {
			log.Printf("eksmo startup bootstrap failed in background: %v", err)
			return
		}

		if result.AlreadyFilled {
			log.Printf(
				"eksmo startup bootstrap skipped: products=%d subjects=%d niches=%d",
				result.ProductsCount,
				result.SubjectsCount,
				result.NichesCount,
			)
			return
		}

		log.Printf(
			"eksmo startup bootstrap completed: fetched=%d upserted=%d modified=%d pages=%d",
			result.SyncResult.Fetched,
			result.SyncResult.Upserted,
			result.SyncResult.Modified,
			result.SyncResult.Pages,
		)
	}()
}
