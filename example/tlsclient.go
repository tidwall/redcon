package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

const rootCert = `-----BEGIN CERTIFICATE-----
MIIB+TCCAZ+gAwIBAgIJAL05LKXo6PrrMAoGCCqGSM49BAMCMFkxCzAJBgNVBAYT
AkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRn
aXRzIFB0eSBMdGQxEjAQBgNVBAMMCWxvY2FsaG9zdDAeFw0xNTEyMDgxNDAxMTNa
Fw0yNTEyMDUxNDAxMTNaMFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0
YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMM
CWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABHGaaHVod0hLOR4d
66xIrtS2TmEmjSFjt+DIEcb6sM9RTKS8TZcdBnEqq8YT7m2sKbV+TEq9Nn7d9pHz
pWG2heWjUDBOMB0GA1UdDgQWBBR0fqrecDJ44D/fiYJiOeBzfoqEijAfBgNVHSME
GDAWgBR0fqrecDJ44D/fiYJiOeBzfoqEijAMBgNVHRMEBTADAQH/MAoGCCqGSM49
BAMCA0gAMEUCIEKzVMF3JqjQjuM2rX7Rx8hancI5KJhwfeKu1xbyR7XaAiEA2UT7
1xOP035EcraRmWPe7tO0LpXgMxlh2VItpc2uc2w=
-----END CERTIFICATE-----
`

var addr = ":6380"

func main() {
	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(rootCert))

	if !ok {
		log.Fatal("failed to parse root certificate")
	}

	config := &tls.Config{RootCAs: roots, ServerName: "localhost"}
	dialOptions := redis.DialTLSConfig(config)

	conn, err := redis.Dial("tcp", addr, dialOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	reply, err := conn.Do("PING")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply)
	// Output: PONG

	reply, err = conn.Do("SET", "key", "hello world")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(reply)
	// Output: OK

	reply, err = conn.Do("GET", "key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply.([]byte)))
	// Output: hello world
}
