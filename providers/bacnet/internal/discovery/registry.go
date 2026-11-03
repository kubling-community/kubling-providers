package discovery

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
)

type registry struct {
	mu         sync.RWMutex
	generation uint64
	scopes     map[string]ScopeSnapshot
}

func newRegistry(scopes []config.Scope) *registry {
	result := &registry{scopes: make(map[string]ScopeSnapshot, len(scopes))}
	for _, scope := range scopes {
		result.scopes[scope.Key()] = ScopeSnapshot{
			Namespace:    scope.Namespace,
			ScopeID:      scope.ID,
			AttachmentID: scope.AttachmentID,
		}
	}
	return result
}

func (r *registry) replace(
	scope config.Scope,
	attemptedAt time.Time,
	observationTTL time.Duration,
	devices []Device,
	duplicates []Duplicate,
	issues []Issue,
	success bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.scopes[scope.Key()]
	devices = mergeRetainedDevices(previous.Devices, devices, duplicates, attemptedAt, observationTTL)
	next := ScopeSnapshot{
		Namespace:    scope.Namespace,
		ScopeID:      scope.ID,
		AttachmentID: scope.AttachmentID,
		LastAttempt:  attemptedAt,
		LastSuccess:  previous.LastSuccess,
		Devices:      cloneDevices(devices),
		Duplicates:   cloneDuplicates(duplicates),
		Issues:       append([]Issue(nil), issues...),
	}
	if success {
		next.LastSuccess = attemptedAt
	}
	r.scopes[scope.Key()] = next
	r.generation++
}

func mergeRetainedDevices(
	previous []Device,
	current []Device,
	duplicates []Duplicate,
	now time.Time,
	ttl time.Duration,
) []Device {
	previousByInstance := make(map[uint32]Device, len(previous))
	for _, device := range previous {
		previousByInstance[device.Instance] = device
	}
	currentInstances := make(map[uint32]struct{}, len(current))
	for index := range current {
		currentInstances[current[index].Instance] = struct{}{}
		old, exists := previousByInstance[current[index].Instance]
		if !exists || !hasIdentityIssue(current[index].Issues) {
			continue
		}
		current[index].Identity = mergeIdentity(current[index].Identity, old.Identity)
		if old.ProfileID != "" {
			current[index].ProfileID = old.ProfileID
		}
		if current[index].LastSeen.IsZero() {
			current[index].LastSeen = old.LastSeen
		}
	}
	duplicateInstances := make(map[uint32]struct{}, len(duplicates))
	for _, duplicate := range duplicates {
		duplicateInstances[duplicate.DeviceInstance] = struct{}{}
	}
	for _, device := range previous {
		if _, exists := currentInstances[device.Instance]; exists {
			continue
		}
		if _, conflicted := duplicateInstances[device.Instance]; conflicted {
			continue
		}
		if device.LastSeen.IsZero() || ttl <= 0 || now.Sub(device.LastSeen) >= ttl {
			continue
		}
		current = append(current, device)
	}
	slices.SortFunc(current, compareDevices)
	return current
}

func hasIssue(issues []Issue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func hasIdentityIssue(issues []Issue) bool {
	return hasIssue(issues, IssueIdentityReadFailed) ||
		hasIssue(issues, IssuePropertyUnavailable)
}

func mergeIdentity(current Identity, previous Identity) Identity {
	if current.VendorIdentifier == 0 {
		current.VendorIdentifier = previous.VendorIdentifier
	}
	if current.VendorName == "" {
		current.VendorName = previous.VendorName
	}
	if current.ModelName == "" {
		current.ModelName = previous.ModelName
	}
	if current.FirmwareRevision == "" {
		current.FirmwareRevision = previous.FirmwareRevision
	}
	if current.ApplicationSoftwareVersion == "" {
		current.ApplicationSoftwareVersion = previous.ApplicationSoftwareVersion
	}
	if current.ProtocolVersion == 0 {
		current.ProtocolVersion = previous.ProtocolVersion
	}
	if current.ProtocolRevision == 0 {
		current.ProtocolRevision = previous.ProtocolRevision
	}
	return current
}

func (r *registry) snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := Snapshot{
		Generation: r.generation,
		CapturedAt: time.Now().UTC(),
		Scopes:     make([]ScopeSnapshot, 0, len(r.scopes)),
	}
	for _, scope := range r.scopes {
		scope.Devices = cloneDevices(scope.Devices)
		scope.Duplicates = cloneDuplicates(scope.Duplicates)
		scope.Issues = append([]Issue(nil), scope.Issues...)
		result.Scopes = append(result.Scopes, scope)
	}
	slices.SortFunc(result.Scopes, func(left ScopeSnapshot, right ScopeSnapshot) int {
		leftKey := left.Namespace + "/" + left.ScopeID
		rightKey := right.Namespace + "/" + right.ScopeID
		return strings.Compare(leftKey, rightKey)
	})
	return result
}

func cloneDevices(input []Device) []Device {
	result := append([]Device(nil), input...)
	for index := range result {
		result[index].Route.MAC = append([]byte(nil), input[index].Route.MAC...)
		result[index].Issues = append([]Issue(nil), input[index].Issues...)
	}
	return result
}

func cloneDuplicates(input []Duplicate) []Duplicate {
	result := append([]Duplicate(nil), input...)
	for index := range result {
		result[index].Routes = append([]Route(nil), input[index].Routes...)
		for routeIndex := range result[index].Routes {
			result[index].Routes[routeIndex].MAC = append(
				[]byte(nil),
				input[index].Routes[routeIndex].MAC...,
			)
		}
	}
	return result
}
