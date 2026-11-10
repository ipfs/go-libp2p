package libp2ptls

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	ic "github.com/libp2p/go-libp2p/core/crypto"
)

// DefaultCertManager is the default certificate manager that creates self-signed certificates
type DefaultCertManager struct{}

// CreateCertificate generates a new ECDSA private key and corresponding x509 certificate.
// The certificate includes an extension that cryptographically ties it to the provided libp2p
// private key to authenticate TLS connections.
func (m *DefaultCertManager) CreateCertificate(privKey ic.PrivKey, template *x509.Certificate) (*tls.Certificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	// Generate the signed extension that binds the certificate to the libp2p key
	extension, err := GenerateSignedExtension(privKey, certKey.Public())
	if err != nil {
		return nil, err
	}

	template.ExtraExtensions = append(template.ExtraExtensions, extension)

	// Self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, certKey.Public(), certKey)
	if err != nil {
		return nil, err
	}

	return &tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  certKey,
	}, nil
}

// CACertManager is a certificate manager that uses a CA to sign certificates
type CACertManager struct {
	CACert             *x509.Certificate
	CAPrivKey          crypto.PrivateKey
	CAPool             *x509.CertPool
	defaultCertManager *DefaultCertManager
}

// NewCACertManager creates a new CA certificate manager from a file
func NewCACertManager(caCertPath string) (*CACertManager, error) {
	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, err
	}

	block, rest := pem.Decode(caCertPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("no valid CA cert found in %s", caCertPath)
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	blockKey, _ := pem.Decode(rest)
	if blockKey == nil {
		return nil, fmt.Errorf("no CA private key found in %s", caCertPath)
	}

	caKey, err := x509.ParsePKCS8PrivateKey(blockKey.Bytes)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	return &CACertManager{
		CACert:    caCert,
		CAPrivKey: caKey,
		CAPool:    caPool,
	}, nil
}

// CreateCertificate generates a CA-signed certificate
func (m *CACertManager) CreateCertificate(privKey ic.PrivKey, template *x509.Certificate) (*tls.Certificate, error) {
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	extension, err := GenerateSignedExtension(privKey, certKey.Public())
	if err != nil {
		return nil, err
	}

	template.ExtraExtensions = append(template.ExtraExtensions, extension)

	// CA-signed certificate (not self-signed)
	certDER, err := x509.CreateCertificate(rand.Reader, template, m.CACert, certKey.Public(), m.CAPrivKey)
	if err != nil {
		return nil, err
	}

	return &tls.Certificate{
		Certificate: [][]byte{certDER, m.CACert.Raw},
		PrivateKey:  certKey,
	}, nil
}

// VerifyCertChain first adds CA verification on top of the default certificate verification
func (m *CACertManager) VerifyCertChain(chain []*x509.Certificate) (ic.PubKey, error) {
	// Ensure there's at least one certificate beyond the CA certificate.
	if len(chain) < 2 {
		return nil, fmt.Errorf("insufficient certificate chain length")
	}

	opts := x509.VerifyOptions{
		Roots:         m.CAPool, // trusted root certificate
		Intermediates: x509.NewCertPool(),
	}

	for _, cert := range chain[1:] {
		opts.Intermediates.AddCert(cert)
	}

	// Verify the leaf certificate
	_, err := chain[0].Verify(opts)
	if err != nil {
		return nil, fmt.Errorf("full chain verification failed: %v", err)
	}

	// Verify first cert against the default cert manager
	pubKey, err := m.defaultCertManager.VerifyCertChain(chain[0:1])
	if err != nil {
		return nil, fmt.Errorf("cert verification failed: %v", err)
	}

	return pubKey, nil
}
