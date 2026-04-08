package netbird

import (
	"fmt"
	"net"
	"os"
)

// netbirdCIDR is the CGNAT range (100.64.0.0/10) that Netbird assigns addresses from.
var netbirdCIDR = mustParseCIDR("100.64.0.0/10")

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

// FindAddr returns the IPv4 address of the Netbird network interface by scanning
// all network interfaces for an address in 100.64.0.0/10.
// In development mode (ENVIRONMENT=development) the scan is skipped and
// INGRESSIVE_LISTEN_ADDR is returned directly so that local testing works
// without a real Netbird install.
// Falls back to the INGRESSIVE_LISTEN_ADDR environment variable if no such
// interface is found.
func FindAddr() (string, error) {
	if os.Getenv("ENVIRONMENT") == "development" {
		if v := os.Getenv("INGRESSIVE_LISTEN_ADDR"); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("ENVIRONMENT=development but INGRESSIVE_LISTEN_ADDR is not set")
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list interfaces: %w", err)
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip := addrIP(addr)
			if ip != nil && ip.To4() != nil && netbirdCIDR.Contains(ip) {
				return ip.String(), nil
			}
		}
	}

	if v := os.Getenv("INGRESSIVE_LISTEN_ADDR"); v != "" {
		return v, nil
	}

	return "", fmt.Errorf("no Netbird interface found (100.64.0.0/10) and INGRESSIVE_LISTEN_ADDR is not set")
}

func addrIP(addr net.Addr) net.IP {
	switch v := addr.(type) {
	case *net.IPNet:
		return v.IP
	case *net.IPAddr:
		return v.IP
	}
	return nil
}
