package libp2ptls

import (
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime/debug"
	"slices"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/sec"
)

const certValidityPeriod = 100 * 365 * 24 * time.Hour // ~100 years
const certificatePrefix = "libp2p-tls-handshake:"
const alpn string = "libp2p"

var extensionID = getPrefixedExtensionID([]int{1, 1})
var extensionCritical bool // so we can mark the extension critical in tests

// CertManager defines an interface for TLS certificate management operations
type CertManager interface {
	// CreateCertificate generates a certificate using the provided private key and template
	CreateCertificate(privateKey ic.PrivKey, template *x509.Certificate) (*tls.Certificate, error)

	// VerifyCertChain verifies the certificate chain and extracts the public key
	VerifyCertChain(chain []*x509.Certificate) (ic.PubKey, error)
}

type signedKey struct {
	PubKey    []byte
	Signature []byte
}

// Identity is used to secure connections
type Identity struct {
	config      tls.Config
	certManager CertManager
}

// IdentityConfig is used to configure an Identity
type IdentityConfig struct {
	CertTemplate *x509.Certificate
	KeyLogWriter io.Writer
	CertManager  CertManager
}

// IdentityOption transforms an IdentityConfig to apply optional settings.
type IdentityOption func(r *IdentityConfig)

// WithCertTemplate specifies the template to use when generating a new certificate.
func WithCertTemplate(template *x509.Certificate) IdentityOption {
	return func(c *IdentityConfig) {
		c.CertTemplate = template
	}
}

// WithCertManager sets a custom certificate manager
func WithCertManager(cm CertManager) IdentityOption {
	return func(c *IdentityConfig) {
		c.CertManager = cm
	}
}

// WithKeyLogWriter optionally specifies a destination for TLS master secrets
// in NSS key log format that can be used to allow external programs
// such as Wireshark to decrypt TLS connections.
// See https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS/Key_Log_Format.
// Use of KeyLogWriter compromises security and should only be
// used for debugging.
func WithKeyLogWriter(w io.Writer) IdentityOption {
	return func(c *IdentityConfig) {
		c.KeyLogWriter = w
	}
}

// NewIdentity creates a new identity
func NewIdentity(privKey ic.PrivKey, opts ...IdentityOption) (*Identity, error) {
	config := IdentityConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	// Use default cert manager if none provided
	if config.CertManager == nil {
		config.CertManager = &DefaultCertManager{}
	}

	if config.CertTemplate == nil {
		var err error
		config.CertTemplate, err = certTemplate()
		if err != nil {
			return nil, err
		}
	}

	cert, err := config.CertManager.CreateCertificate(privKey, config.CertTemplate)
	if err != nil {
		return nil, err
	}

	return &Identity{
		certManager: config.CertManager,
		config: tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: true, // This is not insecure here. We will verify the cert chain ourselves.
			ClientAuth:         tls.RequireAnyClientCert,
			Certificates:       []tls.Certificate{*cert},
			VerifyPeerCertificate: func(_ [][]byte, _ [][]*x509.Certificate) error {
				panic("tls config not specialized for peer")
			},
			NextProtos:             []string{alpn},
			SessionTicketsDisabled: true,
			KeyLogWriter:           config.KeyLogWriter,
		},
	}, nil
}

// CertManager returns the certificate manager used by the identity
// to create and verify certificates.
func (i *Identity) CertManager() CertManager {
	return i.certManager
}

// ConfigForPeer creates a new single-use tls.Config that verifies the peer's
// certificate chain and returns the peer's public key via the channel. If the
// peer ID is empty, the returned config will accept any peer.
//
// It should be used to create a new tls.Config before securing either an
// incoming or outgoing connection.
func (i *Identity) ConfigForPeer(remote peer.ID) (*tls.Config, <-chan ic.PubKey) {
	keyCh := make(chan ic.PubKey, 1)
	// We need to check the peer ID in the VerifyPeerCertificate callback.
	// The tls.Config it is also used for listening, and we might also have concurrent dials.
	// Clone it so we can check for the specific peer ID we're dialing here.
	conf := i.config.Clone()
	// We're using InsecureSkipVerify, so the verifiedChains parameter will always be empty.
	// We need to parse the certificates ourselves from the raw certs.
	conf.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) (err error) {
		defer func() {
			if rerr := recover(); rerr != nil {
				fmt.Fprintf(os.Stderr, "panic when processing peer certificate in TLS handshake: %s\n%s\n", rerr, debug.Stack())
				err = fmt.Errorf("panic when processing peer certificate in TLS handshake: %s", rerr)

			}
		}()

		defer close(keyCh)

		chain := make([]*x509.Certificate, len(rawCerts))
		for i := range rawCerts {
			cert, err := x509.ParseCertificate(rawCerts[i])
			if err != nil {
				return err
			}
			chain[i] = cert
		}

		pubKey, err := i.certManager.VerifyCertChain(chain)
		if err != nil {
			return err
		}

		if remote != "" && !remote.MatchesPublicKey(pubKey) {
			peerID, err := peer.IDFromPublicKey(pubKey)
			if err != nil {
				peerID = peer.ID(fmt.Sprintf("(not determined: %s)", err.Error()))
			}
			return sec.ErrPeerIDMismatch{Expected: remote, Actual: peerID}
		}
		keyCh <- pubKey
		return nil
	}
	return conf, keyCh
}

// PubKeyFromCertChain verifies the certificate chain and extract the remote's public key.
func (m *DefaultCertManager) VerifyCertChain(chain []*x509.Certificate) (ic.PubKey, error) {
	if len(chain) != 1 {
		return nil, errors.New("expected one certificates in the chain")
	}
	cert := chain[0]
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	var found bool
	var keyExt pkix.Extension
	// find the libp2p key extension, skipping all unknown extensions
	for _, ext := range cert.Extensions {
		if extensionIDEqual(ext.Id, extensionID) {
			keyExt = ext
			found = true
			//delete the extension from UnhandledCriticalExtensions
			cert.UnhandledCriticalExtensions = slices.DeleteFunc(cert.UnhandledCriticalExtensions, func(oid asn1.ObjectIdentifier) bool {
				return oid.Equal(ext.Id)
			})

			break
		}
	}
	if !found {
		return nil, errors.New("expected certificate to contain the key extension")
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		// If we return an x509 error here, it will be sent on the wire.
		// Wrap the error to avoid that.
		return nil, fmt.Errorf("certificate verification failed: %s", err)
	}

	var sk signedKey
	if _, err := asn1.Unmarshal(keyExt.Value, &sk); err != nil {
		return nil, fmt.Errorf("unmarshalling signed certificate failed: %s", err)
	}
	pubKey, err := ic.UnmarshalPublicKey(sk.PubKey)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling public key failed: %s", err)
	}
	certKeyPub, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, err
	}
	valid, err := pubKey.Verify(append([]byte(certificatePrefix), certKeyPub...), sk.Signature)
	if err != nil {
		return nil, fmt.Errorf("signature verification failed: %s", err)
	}
	if !valid {
		return nil, errors.New("signature invalid")
	}

	return pubKey, nil
}

// GenerateSignedExtension uses the provided private key to sign the public key, and returns the
// signature within a pkix.Extension.
// This extension is included in a certificate to cryptographically tie it to the libp2p private key.
func GenerateSignedExtension(sk ic.PrivKey, pubKey crypto.PublicKey) (pkix.Extension, error) {
	keyBytes, err := ic.MarshalPublicKey(sk.GetPublic())
	if err != nil {
		return pkix.Extension{}, err
	}
	certKeyPub, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return pkix.Extension{}, err
	}
	signature, err := sk.Sign(append([]byte(certificatePrefix), certKeyPub...))
	if err != nil {
		return pkix.Extension{}, err
	}
	value, err := asn1.Marshal(signedKey{
		PubKey:    keyBytes,
		Signature: signature,
	})
	if err != nil {
		return pkix.Extension{}, err
	}

	return pkix.Extension{Id: extensionID, Critical: extensionCritical, Value: value}, nil
}

// certTemplate returns the template for generating an Identity's TLS certificates.
func certTemplate() (*x509.Certificate, error) {
	bigNum := big.NewInt(1 << 62)
	sn, err := rand.Int(rand.Reader, bigNum)
	if err != nil {
		return nil, err
	}

	subjectSN, err := rand.Int(rand.Reader, bigNum)
	if err != nil {
		return nil, err
	}

	return &x509.Certificate{
		SerialNumber: sn,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValidityPeriod),
		// According to RFC 3280, the issuer field must be set,
		// see https://datatracker.ietf.org/doc/html/rfc3280#section-4.1.2.4.
		Subject: pkix.Name{SerialNumber: subjectSN.String()},
	}, nil
}
