package egress

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/flexer2006/hopper/internal/deliver"
	"github.com/flexer2006/hopper/internal/domain"
)

type Client struct {
	lookup lookupFunc
	dial   dialFunc
	roots  *x509.CertPool
}

type lookupFunc func(ctx context.Context, host string) ([]netip.Addr, error)

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

const (
	dialTimeout  = 5 * time.Second
	tlsTimeout   = 5 * time.Second
	totalTimeout = 10 * time.Second
	maxBody      = 1 << 20
	httpsPort    = "443"
	httpPort     = "80"
)

var (
	_ deliver.HTTP = (*Client)(nil)

	errDialMismatch = errors.New("dial address is not the pinned ip")
)

func New() *Client {
	return &Client{}
}

func (c *Client) Post(ctx context.Context, in deliver.HTTPRequest) (deliver.HTTPResult, error) {
	if in.JobID == "" || in.Cycle < 0 || in.Attempt < 1 {
		return deliver.HTTPResult{}, ErrInvalidRequest
	}

	parsed, err := domain.ParseTarget(in.Target)
	if err != nil {
		return deliver.HTTPResult{}, err
	}

	host := parsed.Hostname()
	port := parsed.Port()

	if port == "" {
		port = httpPort
		if parsed.Scheme == "https" {
			port = httpsPort
		}
	}

	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	pin, err := c.resolve(ctx, host, port)
	if err != nil {
		return deliver.HTTPResult{}, err
	}

	return c.roundTrip(ctx, pin, host, parsed, in)
}

func (c *Client) roundTrip(
	ctx context.Context,
	pin netip.AddrPort,
	serverName string,
	parsed *url.URL,
	in deliver.HTTPRequest,
) (deliver.HTTPResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(in.Payload))
	if err != nil {
		return deliver.HTTPResult{}, fmt.Errorf("egress request: %w", err)
	}

	req.URL.Host = net.JoinHostPort(pin.Addr().String(), strconv.Itoa(int(pin.Port())))
	req.Host = parsed.Host
	req.Header.Set("Idempotency-Key", domain.OutboundIdempotencyKey(in.JobID, in.Cycle, in.Attempt))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{ //nolint:exhaustruct_v5 // Timeout is the request context; Jar unused
		Transport:     c.transport(pin, serverName),
		CheckRedirect: rejectRedirect,
	}

	resp, err := client.Do(req)
	if err != nil {
		return deliver.HTTPResult{}, fmt.Errorf("egress transport: %w", scrubTransport(err))
	}

	n, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody+1))
	closeErr := resp.Body.Close()

	if readErr != nil {
		return deliver.HTTPResult{}, fmt.Errorf("egress read: %w", readErr)
	}

	if closeErr != nil {
		return deliver.HTTPResult{}, fmt.Errorf("egress close: %w", closeErr)
	}

	if n > maxBody {
		return deliver.HTTPResult{}, ErrBodyLimit
	}

	return deliver.HTTPResult{StatusCode: resp.StatusCode, BytesRead: int(n)}, nil
}

func (c *Client) transport(pin netip.AddrPort, serverName string) *http.Transport {
	protos := new(http.Protocols)
	protos.SetHTTP1(true)

	cfg := &tls.Config{ //nolint:exhaustruct_v5 // InsecureSkipVerify stays false; system or injected roots
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
		RootCAs:    c.roots,
	}

	want := pin.String()

	return &http.Transport{ //nolint:exhaustruct_v5 // per-attempt isolated transport; zeros are protocol defaults
		Proxy:                 noProxy,
		ForceAttemptHTTP2:     false,
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   tlsTimeout,
		TLSNextProto:          map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
		Protocols:             protos,
		TLSClientConfig:       cfg,
		DialContext:           c.pinDial(want),
		ResponseHeaderTimeout: totalTimeout,
	}
}

func (c *Client) pinDial(want string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, errDialMismatch
		}

		if addr != want {
			return nil, errDialMismatch
		}

		return c.dialTCP(ctx, network, addr)
	}
}

func (c *Client) dialTCP(ctx context.Context, network, addr string) (net.Conn, error) {
	if c.dial != nil {
		return c.dial(ctx, network, addr)
	}

	d := net.Dialer{Timeout: dialTimeout} //nolint:exhaustruct_v5 // Deadline/KeepAlive zeros are fine

	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("egress dial: %w", err)
	}

	return conn, nil
}

func (c *Client) resolve(ctx context.Context, host, port string) (netip.AddrPort, error) {
	ip, parseErr := netip.ParseAddr(host)
	if parseErr == nil {
		if denied(ip) {
			return netip.AddrPort{}, ErrBlocked
		}

		return parseAddrPort(ip, port)
	}

	addrs, err := c.lookupHost(ctx, host)
	if err != nil {
		return netip.AddrPort{}, err
	}

	if len(addrs) == 0 {
		return netip.AddrPort{}, ErrEmptyResolution
	}

	if slices.ContainsFunc(addrs, denied) {
		return netip.AddrPort{}, ErrBlocked
	}

	return parseAddrPort(addrs[0], port)
}

func (c *Client) lookupHost(ctx context.Context, host string) ([]netip.Addr, error) {
	var (
		addrs []netip.Addr
		err   error
	)

	if c.lookup != nil {
		addrs, err = c.lookup(ctx, host)
	} else {
		addrs, err = net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	}

	if err != nil {
		return nil, fmt.Errorf("egress lookup: %w", scrubLookup(err))
	}

	return addrs, nil
}

func rejectRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func noProxy(_ *http.Request) (*url.URL, error) {
	return nil, nil //nolint:nilnil // Transport.Proxy: nil URL disables HTTP_PROXY (never DefaultTransport)
}

func scrubTransport(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err
	}

	return err
}

func scrubLookup(err error) error {
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) {
		return err
	}

	clone := *dnsErr
	clone.Name = ""
	clone.Server = ""

	return &clone
}

func parseAddrPort(ip netip.Addr, port string) (netip.AddrPort, error) {
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	return netip.AddrPortFrom(ip.Unmap().WithZone(""), uint16(n)), nil
}
