package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"

	"github.com/octarhq/octar/internal/auth/authenticator"
	"github.com/octarhq/octar/internal/auth/identity"
	"github.com/octarhq/octar/internal/config"
)

type MTLSAuthenticator struct {
	config      config.MTLSConfig
	clientCAs   *x509.CertPool
	allowedCNs  []string
	allowedSANs []string
}

func NewMTLSAuthenticator(cfg config.MTLSConfig) *MTLSAuthenticator {
	pool := x509.NewCertPool()
	if cfg.ClientCACert != "" {
		pool.AppendCertsFromPEM([]byte(cfg.ClientCACert))
	}

	return &MTLSAuthenticator{
		config:      cfg,
		clientCAs:   pool,
		allowedCNs:  cfg.AllowedCNs,
		allowedSANs: cfg.AllowedSANs,
	}
}

func (m *MTLSAuthenticator) Name() string {
	return "mtls"
}

func (m *MTLSAuthenticator) Priority() int {
	return 5 // Highest priority - most secure
}

func (m *MTLSAuthenticator) Authenticate(ctx context.Context, req authenticator.AuthRequest) (*identity.Identity, string, error) {
	if req.Certificate == "" {
		return nil, "", nil
	}

	cert, err := m.parseCertificate(req.Certificate)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse certificate: %w", err)
	}

	if !m.isCertificateAllowed(cert) {
		return nil, "", fmt.Errorf("certificate not in allowlist")
	}

	subjectID := m.extractSubjectID(cert)
	namespace := m.extractNamespace(cert)

	id := &identity.Identity{
		SubjectID:   subjectID,
		SubjectType: identity.SubjectService,
		AccountID:   subjectID,
		Namespace:   namespace,
		Roles:       []string{"service"},
		AuthMethod:  identity.AuthMethodMTLS,
		Namespaces:  map[string][]string{namespace: {"publish", "consume", "ack", "nack"}},
		Metadata:    map[string]string{"serial": cert.SerialNumber.String()},
	}

	return id, "", nil
}

func (m *MTLSAuthenticator) parseCertificate(certPEM string) (*x509.Certificate, error) {
	cert, err := decodePEM([]byte(certPEM))
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func (m *MTLSAuthenticator) isCertificateAllowed(cert *x509.Certificate) bool {
	// Check CN allowlist
	for _, cn := range m.allowedCNs {
		if cert.Subject.CommonName == cn {
			return true
		}
	}

	// Check SAN allowlist
	for _, san := range cert.DNSNames {
		for _, allowed := range m.allowedSANs {
			if san == allowed {
				return true
			}
		}
	}

	// If no allowlist configured, allow all
	return len(m.allowedCNs) == 0 && len(m.allowedSANs) == 0
}

func (m *MTLSAuthenticator) extractSubjectID(cert *x509.Certificate) string {
	if cert.Subject.CommonName != "" {
		return cert.Subject.CommonName
	}
	if len(cert.DNSNames) > 0 {
		return cert.DNSNames[0]
	}
	return cert.Subject.Organization[0]
}

func (m *MTLSAuthenticator) extractNamespace(cert *x509.Certificate) string {
	// Extract namespace from certificate extension or default
	for _, ext := range cert.Extensions {
		if ext.Id.String() == "1.2.3.4.5.6.7.8.1" { // Example OID for namespace
			return string(ext.Value)
		}
	}
	return "default"
}

func (m *MTLSAuthenticator) VerifyConnection(conn net.Conn) (*identity.Identity, error) {
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return nil, fmt.Errorf("connection is not TLS")
	}

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no client certificate provided")
	}

	cert := state.PeerCertificates[0]

	if !m.isCertificateAllowed(cert) {
		return nil, fmt.Errorf("certificate not allowed")
	}

	subjectID := m.extractSubjectID(cert)
	namespace := m.extractNamespace(cert)

	return &identity.Identity{
		SubjectID:   subjectID,
		SubjectType: identity.SubjectService,
		AccountID:   subjectID,
		Namespace:   namespace,
		Roles:       []string{"service"},
		AuthMethod:  identity.AuthMethodMTLS,
		Namespaces:  map[string][]string{namespace: {"publish", "consume", "ack", "nack"}},
		Metadata:    map[string]string{"serial": cert.SerialNumber.String()},
	}, nil
}

func decodePEM(data []byte) (*x509.Certificate, error) {
	var block *pem.Block
	var rest []byte
	for {
		block, rest = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}
			return cert, nil
		}
		data = rest
	}
	return nil, fmt.Errorf("no certificate found in PEM")
}
