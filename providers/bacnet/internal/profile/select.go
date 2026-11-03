package profile

import (
	"fmt"
	"strings"
)

// Select returns the most specific profile matching the device identity. The
// built-in standard profile is the fallback when no injected profile matches.
func (c *Catalog) Select(identity DeviceIdentity) (*Profile, error) {
	if c == nil || c.fallback == nil {
		return nil, fmt.Errorf("BACnet profile catalog has no fallback profile")
	}
	selected := c.fallback
	selectedRank := uint8(0)
	for _, candidate := range c.ordered {
		if candidate.Abstract || candidate.ID == StandardProfileID {
			continue
		}
		candidateRank := uint8(0)
		for _, selector := range candidate.Selectors {
			if selector.matches(identity) && selector.rank() > candidateRank {
				candidateRank = selector.rank()
			}
		}
		if candidateRank == 0 {
			continue
		}
		if candidateRank == selectedRank {
			return nil, fmt.Errorf(
				"device identity matches profiles %q and %q at the same precedence",
				selected.ID,
				candidate.ID,
			)
		}
		if candidateRank > selectedRank {
			selected = candidate
			selectedRank = candidateRank
		}
	}
	return selected, nil
}

func (s Selector) rank() uint8 {
	if s.DeviceInstance != nil {
		return 4
	}
	if s.FirmwareRevision != "" {
		return 3
	}
	if s.ModelName != "" {
		return 2
	}
	if s.VendorIdentifier != nil {
		return 1
	}
	return 0
}

func (s Selector) matches(identity DeviceIdentity) bool {
	if s.VendorIdentifier != nil && *s.VendorIdentifier != identity.VendorIdentifier {
		return false
	}
	if s.ModelName != "" && s.ModelName != strings.TrimSpace(identity.ModelName) {
		return false
	}
	if s.FirmwareRevision != "" &&
		s.FirmwareRevision != strings.TrimSpace(identity.FirmwareRevision) {
		return false
	}
	if s.Namespace != "" && s.Namespace != strings.TrimSpace(identity.Namespace) {
		return false
	}
	if s.ScopeID != "" && s.ScopeID != strings.TrimSpace(identity.ScopeID) {
		return false
	}
	return s.DeviceInstance == nil || *s.DeviceInstance == identity.DeviceInstance
}

func selectorsOverlap(left Selector, right Selector) bool {
	if left.VendorIdentifier != nil && right.VendorIdentifier != nil &&
		*left.VendorIdentifier != *right.VendorIdentifier {
		return false
	}
	if left.ModelName != "" && right.ModelName != "" && left.ModelName != right.ModelName {
		return false
	}
	if left.FirmwareRevision != "" && right.FirmwareRevision != "" &&
		left.FirmwareRevision != right.FirmwareRevision {
		return false
	}
	if left.Namespace != "" && right.Namespace != "" && left.Namespace != right.Namespace {
		return false
	}
	if left.ScopeID != "" && right.ScopeID != "" && left.ScopeID != right.ScopeID {
		return false
	}
	return left.DeviceInstance == nil || right.DeviceInstance == nil ||
		*left.DeviceInstance == *right.DeviceInstance
}

func validateSelectorAmbiguity(profiles []*Profile) error {
	for leftIndex, left := range profiles {
		if left.Abstract || left.ID == StandardProfileID {
			continue
		}
		for _, right := range profiles[leftIndex+1:] {
			if right.Abstract || right.ID == StandardProfileID {
				continue
			}
			for _, leftSelector := range left.Selectors {
				for _, rightSelector := range right.Selectors {
					if leftSelector.rank() == rightSelector.rank() &&
						selectorsOverlap(leftSelector, rightSelector) {
						return fmt.Errorf(
							"profiles %q and %q have ambiguous selectors at precedence %d",
							left.ID,
							right.ID,
							leftSelector.rank(),
						)
					}
				}
			}
		}
	}
	return nil
}

func validateInheritancePrecedence(profiles map[string]*Profile) error {
	for _, child := range profiles {
		if child.Abstract || child.ID == StandardProfileID {
			continue
		}
		for _, parentID := range child.Extends {
			parent := profiles[parentID]
			if parent == nil || parent.Abstract || parent.ID == StandardProfileID {
				continue
			}
			for _, childSelector := range child.Selectors {
				for _, parentSelector := range parent.Selectors {
					if selectorsOverlap(childSelector, parentSelector) &&
						childSelector.rank() <= parentSelector.rank() {
						return fmt.Errorf(
							"profile %q must use a more specific selector than inherited profile %q",
							child.ID,
							parent.ID,
						)
					}
				}
			}
		}
	}
	return nil
}
