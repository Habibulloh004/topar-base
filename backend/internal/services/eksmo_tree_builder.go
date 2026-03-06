package services

import (
	"context"
	"sort"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
)

// EksmoTreeBuilder builds hierarchical tree from niches and subjects
type EksmoTreeBuilder struct {
	nicheRepo   *repository.EksmoNicheRepository
	subjectRepo *repository.EksmoSubjectRepository
}

func NewEksmoTreeBuilder(nicheRepo *repository.EksmoNicheRepository, subjectRepo *repository.EksmoSubjectRepository) *EksmoTreeBuilder {
	return &EksmoTreeBuilder{
		nicheRepo:   nicheRepo,
		subjectRepo: subjectRepo,
	}
}

// BuildTree creates a hierarchical tree of niches with subjects as leaves
func (b *EksmoTreeBuilder) BuildTree(ctx context.Context) ([]models.EksmoNicheNode, error) {
	// Fetch all niches
	niches, err := b.nicheRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// Fetch all subjects
	subjects, err := b.subjectRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// Build maps for quick lookup
	nicheMap := make(map[string]models.EksmoNicheEntity, len(niches))
	nicheChildren := make(map[string][]string)      // ownerGuid -> list of child niche guids
	subjectsByNiche := make(map[string][]models.EksmoSubjectRef) // niche guid -> list of subjects

	for _, niche := range niches {
		nicheMap[niche.GUID] = niche
		nicheChildren[niche.OwnerGUID] = append(nicheChildren[niche.OwnerGUID], niche.GUID)
	}

	for _, subject := range subjects {
		subjectsByNiche[subject.OwnerGUID] = append(subjectsByNiche[subject.OwnerGUID], models.EksmoSubjectRef{
			GUID: subject.GUID,
			Name: subject.Name,
		})
	}

	// Find root niches (those whose ownerGuid doesn't exist in nicheMap)
	var rootGuids []string
	for guid, niche := range nicheMap {
		if niche.OwnerGUID == "" {
			rootGuids = append(rootGuids, guid)
		} else if _, exists := nicheMap[niche.OwnerGUID]; !exists {
			// OwnerGUID doesn't point to any known niche, so this is a root
			rootGuids = append(rootGuids, guid)
		}
	}

	// Build tree recursively
	var buildNode func(guid string) models.EksmoNicheNode
	buildNode = func(guid string) models.EksmoNicheNode {
		niche := nicheMap[guid]
		node := models.EksmoNicheNode{
			GUID: niche.GUID,
			Name: niche.Name,
			Type: "niche",
		}

		// Add child niches
		childGuids := nicheChildren[guid]
		sort.Strings(childGuids)
		for _, childGuid := range childGuids {
			childNode := buildNode(childGuid)
			node.Children = append(node.Children, childNode)
		}

		// Add subjects for this niche
		if subs, ok := subjectsByNiche[guid]; ok {
			// Sort subjects by name
			sort.Slice(subs, func(i, j int) bool {
				return subs[i].Name < subs[j].Name
			})
			node.Subjects = subs
		}

		return node
	}

	// Build root nodes
	var tree []models.EksmoNicheNode
	sort.Strings(rootGuids)
	for _, guid := range rootGuids {
		node := buildNode(guid)
		tree = append(tree, node)
	}

	// Sort by name
	sort.Slice(tree, func(i, j int) bool {
		return tree[i].Name < tree[j].Name
	})

	return tree, nil
}

// GetAllDescendantNicheGUIDs returns all niche GUIDs under a given niche (including itself)
func (b *EksmoTreeBuilder) GetAllDescendantNicheGUIDs(ctx context.Context, nicheGUID string) ([]string, error) {
	niches, err := b.nicheRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// Build parent -> children map
	nicheChildren := make(map[string][]string)
	for _, niche := range niches {
		nicheChildren[niche.OwnerGUID] = append(nicheChildren[niche.OwnerGUID], niche.GUID)
	}

	// Collect all descendants recursively
	var result []string
	var collect func(guid string)
	collect = func(guid string) {
		result = append(result, guid)
		for _, childGuid := range nicheChildren[guid] {
			collect(childGuid)
		}
	}

	collect(nicheGUID)
	return result, nil
}

// GetAllSubjectGUIDsForNiche returns all subject GUIDs under a niche and its descendants
func (b *EksmoTreeBuilder) GetAllSubjectGUIDsForNiche(ctx context.Context, nicheGUID string) ([]string, error) {
	nicheGUIDs, err := b.GetAllDescendantNicheGUIDs(ctx, nicheGUID)
	if err != nil {
		return nil, err
	}

	subjects, err := b.subjectRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// Build a set of niche GUIDs for quick lookup
	nicheSet := make(map[string]struct{}, len(nicheGUIDs))
	for _, guid := range nicheGUIDs {
		nicheSet[guid] = struct{}{}
	}

	// Collect subjects whose ownerGuid is in nicheSet
	var result []string
	for _, subject := range subjects {
		if _, ok := nicheSet[subject.OwnerGUID]; ok {
			result = append(result, subject.GUID)
		}
	}

	return result, nil
}
