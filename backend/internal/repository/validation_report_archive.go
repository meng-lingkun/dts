package repository

import (
	"context"
	"errors"
	"strings"

	"qmigration/backend/internal/domain"
)

var ErrValidationReportArchiveConflict = errors.New("validation report archive record conflicts with immutable existing record")

type ValidationReportArchiveProvider interface {
	GetValidationReportArchive(context.Context, string, string) (*domain.ValidationReportArchiveRecord, error)
	CreateValidationReportArchive(context.Context, *domain.ValidationReportArchiveRecord) (bool, error)
}

func ValidationReportArchiveEqual(a, b *domain.ValidationReportArchiveRecord) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.TaskID == b.TaskID &&
		strings.EqualFold(a.EvidenceDigest, b.EvidenceDigest) &&
		a.URI == b.URI && a.Bucket == b.Bucket && a.Prefix == b.Prefix &&
		strings.EqualFold(a.ManifestSHA256, b.ManifestSHA256) &&
		a.PublicSignatureAlgorithm == b.PublicSignatureAlgorithm &&
		a.PublicSignatureKeyID == b.PublicSignatureKeyID &&
		a.PublicKeyEd25519 == b.PublicKeyEd25519 &&
		strings.EqualFold(a.PublicKeyFingerprintSHA256, b.PublicKeyFingerprintSHA256) &&
		a.ObjectLockMode == b.ObjectLockMode && a.RetainUntil == b.RetainUntil &&
		a.LegalHold == b.LegalHold
}
