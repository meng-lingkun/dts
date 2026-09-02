//go:build linux && cgo

package gbase8scdc

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef uint32_t (*qm_abi_version_fn)(void);
typedef void* (*qm_open_fn)(const char*, char**);
typedef int (*qm_health_fn)(void*, char**);
typedef char* (*qm_json_fn)(void*, const char*, char**);
typedef void (*qm_free_fn)(char*);
typedef void (*qm_close_fn)(void*);

typedef struct qm_native_provider {
    void *library;
    void *handle;
    qm_health_fn health;
    qm_json_fn checkpoint;
    qm_json_fn read;
    qm_free_fn provider_free;
    qm_close_fn close;
} qm_native_provider;

static char* qm_dup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *p = (char*)malloc(n);
    if (p) memcpy(p, s, n);
    return p;
}

static void* qm_sym(void *lib, const char *name, char **err) {
    dlerror();
    void *p = dlsym(lib, name);
    const char *e = dlerror();
    if (e != NULL) {
        if (err) *err = qm_dup(e);
        return NULL;
    }
    return p;
}

static qm_native_provider* qm_native_load(const char *path, const char *config_json, char **err) {
    if (err) *err = NULL;
    void *lib = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!lib) {
        if (err) *err = qm_dup(dlerror());
        return NULL;
    }
    qm_abi_version_fn abi = (qm_abi_version_fn)qm_sym(lib, "qm_gbase8s_cdc_abi_version", err);
    if (!abi) { dlclose(lib); return NULL; }
    if (abi() != 4u) {
        if (err) *err = qm_dup("GBase 8s CDC provider ABI version is not 4");
        dlclose(lib);
        return NULL;
    }
    qm_open_fn openf = (qm_open_fn)qm_sym(lib, "qm_gbase8s_cdc_open", err);
    if (!openf) { dlclose(lib); return NULL; }
    qm_health_fn health = (qm_health_fn)qm_sym(lib, "qm_gbase8s_cdc_health", err);
    if (!health) { dlclose(lib); return NULL; }
    qm_json_fn checkpoint = (qm_json_fn)qm_sym(lib, "qm_gbase8s_cdc_checkpoint", err);
    if (!checkpoint) { dlclose(lib); return NULL; }
    qm_json_fn readf = (qm_json_fn)qm_sym(lib, "qm_gbase8s_cdc_read", err);
    if (!readf) { dlclose(lib); return NULL; }
    qm_free_fn freef = (qm_free_fn)qm_sym(lib, "qm_gbase8s_cdc_free", err);
    if (!freef) { dlclose(lib); return NULL; }
    qm_close_fn closef = (qm_close_fn)qm_sym(lib, "qm_gbase8s_cdc_close", err);
    if (!closef) { dlclose(lib); return NULL; }

    char *provider_err = NULL;
    void *handle = openf(config_json ? config_json : "{}", &provider_err);
    if (!handle) {
        if (err) *err = qm_dup(provider_err ? provider_err : "GBase 8s CDC provider open failed");
        if (provider_err) freef(provider_err);
        dlclose(lib);
        return NULL;
    }
    if (provider_err) freef(provider_err);

    qm_native_provider *p = (qm_native_provider*)calloc(1, sizeof(qm_native_provider));
    if (!p) {
        closef(handle);
        dlclose(lib);
        if (err) *err = qm_dup("out of memory loading GBase 8s CDC provider");
        return NULL;
    }
    p->library = lib;
    p->handle = handle;
    p->health = health;
    p->checkpoint = checkpoint;
    p->read = readf;
    p->provider_free = freef;
    p->close = closef;
    return p;
}

static int qm_native_health(qm_native_provider *p, char **err) {
    if (err) *err = NULL;
    char *provider_err = NULL;
    int rc = p->health(p->handle, &provider_err);
    if (rc != 0 && err) *err = qm_dup(provider_err ? provider_err : "GBase 8s CDC provider health failed");
    if (provider_err) p->provider_free(provider_err);
    return rc;
}

static char* qm_native_json(qm_native_provider *p, int which, const char *request, char **err) {
    if (err) *err = NULL;
    char *provider_err = NULL;
    char *provider_out = which == 1 ? p->checkpoint(p->handle, request, &provider_err) : p->read(p->handle, request, &provider_err);
    if (!provider_out) {
        if (err) *err = qm_dup(provider_err ? provider_err : "GBase 8s CDC provider returned no response");
        if (provider_err) p->provider_free(provider_err);
        return NULL;
    }
    if (provider_err) p->provider_free(provider_err);
    const size_t max_response = (size_t)64 * 1024 * 1024;
    size_t n = strnlen(provider_out, max_response + 1);
    if (n > max_response) {
        p->provider_free(provider_out);
        if (err) *err = qm_dup("GBase 8s CDC native provider JSON response exceeds 64 MiB");
        return NULL;
    }
    char *out = (char*)malloc(n + 1);
    if (out) memcpy(out, provider_out, n + 1);
    p->provider_free(provider_out);
    if (!out && err) *err = qm_dup("out of memory copying GBase 8s CDC provider response");
    return out;
}

static void qm_native_close(qm_native_provider *p) {
    if (!p) return;
    if (p->handle && p->close) p->close(p->handle);
    if (p->library) dlclose(p->library);
    free(p);
}
*/
import "C"

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"
)

type nativeProvider struct {
	ptr    *C.qm_native_provider
	mu     sync.Mutex
	pinned bool
}

func verifyNativeLibrary(path, wantSHA256 string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("GBase 8s CDC native provider library path is required")
	}
	if !filepath.IsAbs(path) {
		return "", errors.New("GBase 8s CDC native provider library path must be absolute")
	}
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() {
		return "", fmt.Errorf("GBase 8s CDC native provider is not a regular file: %s", path)
	}
	if st.Mode().Perm()&0o002 != 0 {
		return "", fmt.Errorf("GBase 8s CDC native provider must not be world-writable: %s", path)
	}
	wantSHA256 = strings.ToLower(strings.TrimSpace(wantSHA256))
	if wantSHA256 != "" {
		if len(wantSHA256) != 64 {
			return "", errors.New("GBase 8s CDC native provider SHA-256 must be 64 hex characters")
		}
		if _, err := hex.DecodeString(wantSHA256); err != nil {
			return "", fmt.Errorf("invalid GBase 8s CDC native provider SHA-256: %w", err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		got := fmt.Sprintf("%x", sha256.Sum256(b))
		if got != wantSHA256 {
			return "", fmt.Errorf("GBase 8s CDC native provider SHA-256 mismatch: got %s want %s", got, wantSHA256)
		}
	}
	return filepath.Clean(path), nil
}

// OpenNativeProvider loads a stable C ABI provider. The provider may be built
// with GBase CSDK/ESQL-C independently from the Go toolchain. configJSON is
// local agent configuration and is never supplied by the QMigration control
// plane unless the operator explicitly configures it on the agent host.
func OpenNativeProvider(path, wantSHA256, configJSON string) (Agent, error) {
	clean, err := verifyNativeLibrary(path, wantSHA256)
	if err != nil {
		return nil, err
	}
	configJSON = strings.TrimSpace(configJSON)
	if configJSON == "" {
		configJSON = "{}"
	}
	var tmp any
	if err := json.Unmarshal([]byte(configJSON), &tmp); err != nil {
		return nil, fmt.Errorf("invalid GBase 8s CDC native provider config JSON: %w", err)
	}
	cpath := C.CString(clean)
	cconfig := C.CString(configJSON)
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cconfig))
	var cerr *C.char
	p := C.qm_native_load(cpath, cconfig, &cerr)
	if cerr != nil {
		defer C.free(unsafe.Pointer(cerr))
	}
	if p == nil {
		if cerr != nil {
			return nil, errors.New(C.GoString(cerr))
		}
		return nil, errors.New("failed to load GBase 8s CDC native provider")
	}
	return &nativeProvider{ptr: p, pinned: strings.TrimSpace(wantSHA256) != ""}, nil
}

func (p *nativeProvider) ProviderInfo() ProviderInfo {
	return ProviderInfo{Kind: "native-c-abi", ABIVersion: "4", SHA256Pinned: p.pinned}
}

func (p *nativeProvider) Health(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr == nil {
		return errors.New("GBase 8s CDC native provider is closed")
	}
	var cerr *C.char
	rc := C.qm_native_health(p.ptr, &cerr)
	if cerr != nil {
		defer C.free(unsafe.Pointer(cerr))
	}
	if rc != 0 {
		if cerr != nil {
			return errors.New(C.GoString(cerr))
		}
		return fmt.Errorf("GBase 8s CDC native provider health returned %d", int(rc))
	}
	return nil
}

func (p *nativeProvider) callJSON(which int, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	creq := C.CString(string(b))
	defer C.free(unsafe.Pointer(creq))
	var cerr *C.char
	cresp := C.qm_native_json(p.ptr, C.int(which), creq, &cerr)
	if cerr != nil {
		defer C.free(unsafe.Pointer(cerr))
	}
	if cresp == nil {
		if cerr != nil {
			return errors.New(C.GoString(cerr))
		}
		return errors.New("GBase 8s CDC native provider returned no response")
	}
	defer C.free(unsafe.Pointer(cresp))
	if err := json.Unmarshal([]byte(C.GoString(cresp)), out); err != nil {
		return fmt.Errorf("invalid GBase 8s CDC native provider JSON: %w", err)
	}
	return nil
}

func (p *nativeProvider) Checkpoint(_ context.Context, req CheckpointRequest) (*CheckpointResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr == nil {
		return nil, errors.New("GBase 8s CDC native provider is closed")
	}
	var out CheckpointResponse
	if err := p.callJSON(1, req, &out); err != nil {
		return nil, err
	}
	if err := ValidateCheckpointResponse(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *nativeProvider) Read(_ context.Context, req ReadRequest) (*ReadResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr == nil {
		return nil, errors.New("GBase 8s CDC native provider is closed")
	}
	var out ReadResponse
	if err := p.callJSON(2, req, &out); err != nil {
		return nil, err
	}
	if err := ValidateReadResponse(req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *nativeProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr != nil {
		C.qm_native_close(p.ptr)
		p.ptr = nil
	}
	return nil
}
