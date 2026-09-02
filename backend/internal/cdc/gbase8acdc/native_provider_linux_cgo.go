//go:build linux && cgo

package gbase8acdc

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
typedef int (*qm_ack_fn)(void*, const char*, char**);
typedef void (*qm_free_fn)(char*);
typedef void (*qm_close_fn)(void*);

typedef struct qm_native_provider {
  void *library; void *handle; qm_health_fn health; qm_json_fn checkpoint; qm_json_fn read;
  qm_ack_fn ack; qm_free_fn provider_free; qm_close_fn close;
} qm_native_provider;
static char* qm_dup(const char *s){ if(!s)return NULL; size_t n=strlen(s)+1; char *p=(char*)malloc(n); if(p)memcpy(p,s,n); return p; }
static void* qm_sym(void *lib,const char *name,char **err){ dlerror(); void *p=dlsym(lib,name); const char *e=dlerror(); if(e){ if(err)*err=qm_dup(e); return NULL;} return p; }
static qm_native_provider* qm_load(const char *path,const char *config,char **err){
 if(err)*err=NULL; void *lib=dlopen(path,RTLD_NOW|RTLD_LOCAL); if(!lib){if(err)*err=qm_dup(dlerror());return NULL;}
 qm_abi_version_fn abi=(qm_abi_version_fn)qm_sym(lib,"qm_gbase8a_cdc_abi_version",err); if(!abi){dlclose(lib);return NULL;}
 if(abi()!=1u){if(err)*err=qm_dup("GBase 8a CDC provider ABI version is not 1");dlclose(lib);return NULL;}
 qm_open_fn openf=(qm_open_fn)qm_sym(lib,"qm_gbase8a_cdc_open",err); if(!openf){dlclose(lib);return NULL;}
 qm_health_fn health=(qm_health_fn)qm_sym(lib,"qm_gbase8a_cdc_health",err); if(!health){dlclose(lib);return NULL;}
 qm_json_fn checkpoint=(qm_json_fn)qm_sym(lib,"qm_gbase8a_cdc_checkpoint",err); if(!checkpoint){dlclose(lib);return NULL;}
 qm_json_fn readf=(qm_json_fn)qm_sym(lib,"qm_gbase8a_cdc_read",err); if(!readf){dlclose(lib);return NULL;}
 qm_ack_fn ack=(qm_ack_fn)qm_sym(lib,"qm_gbase8a_cdc_ack",err); if(!ack){dlclose(lib);return NULL;}
 qm_free_fn freef=(qm_free_fn)qm_sym(lib,"qm_gbase8a_cdc_free",err); if(!freef){dlclose(lib);return NULL;}
 qm_close_fn closef=(qm_close_fn)qm_sym(lib,"qm_gbase8a_cdc_close",err); if(!closef){dlclose(lib);return NULL;}
 char *pe=NULL; void *handle=openf(config?config:"{}",&pe); if(!handle){if(err)*err=qm_dup(pe?pe:"GBase 8a CDC provider open failed");if(pe)freef(pe);dlclose(lib);return NULL;} if(pe)freef(pe);
 qm_native_provider *p=(qm_native_provider*)calloc(1,sizeof(qm_native_provider)); if(!p){closef(handle);dlclose(lib);if(err)*err=qm_dup("out of memory");return NULL;}
 p->library=lib;p->handle=handle;p->health=health;p->checkpoint=checkpoint;p->read=readf;p->ack=ack;p->provider_free=freef;p->close=closef;return p;
}
static int qm_health(qm_native_provider *p,char **err){char *pe=NULL;int rc=p->health(p->handle,&pe);if(rc!=0&&err)*err=qm_dup(pe?pe:"health failed");if(pe)p->provider_free(pe);return rc;}
static char* qm_json(qm_native_provider *p,int which,const char *req,char **err){char *pe=NULL;char *po=which==1?p->checkpoint(p->handle,req,&pe):p->read(p->handle,req,&pe);if(!po){if(err)*err=qm_dup(pe?pe:"provider returned no response");if(pe)p->provider_free(pe);return NULL;}if(pe)p->provider_free(pe);size_t max=(size_t)64*1024*1024;size_t n=strnlen(po,max+1);if(n>max){p->provider_free(po);if(err)*err=qm_dup("provider JSON response exceeds 64 MiB");return NULL;}char *out=(char*)malloc(n+1);if(out)memcpy(out,po,n+1);p->provider_free(po);return out;}
static int qm_ack(qm_native_provider *p,const char *req,char **err){char *pe=NULL;int rc=p->ack(p->handle,req,&pe);if(rc!=0&&err)*err=qm_dup(pe?pe:"ack failed");if(pe)p->provider_free(pe);return rc;}
static void qm_close(qm_native_provider *p){if(!p)return;if(p->handle&&p->close)p->close(p->handle);if(p->library)dlclose(p->library);free(p);}
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
	ptr *C.qm_native_provider
	mu  sync.Mutex
}

func verifyNativeLibrary(path, want string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return "", errors.New("GBase 8a CDC native provider requires an absolute library path")
	}
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o002 != 0 {
		return "", errors.New("GBase 8a CDC native provider must be a non-world-writable regular file")
	}
	want = strings.ToLower(strings.TrimSpace(want))
	if want != "" {
		if len(want) != 64 {
			return "", errors.New("GBase 8a CDC provider SHA-256 must be 64 hex chars")
		}
		if _, err := hex.DecodeString(want); err != nil {
			return "", err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		got := fmt.Sprintf("%x", sha256.Sum256(b))
		if got != want {
			return "", fmt.Errorf("GBase 8a CDC provider SHA-256 mismatch: got %s want %s", got, want)
		}
	}
	return filepath.Clean(path), nil
}
func OpenNativeProvider(path, wantSHA256, configJSON string) (Agent, error) {
	clean, err := verifyNativeLibrary(path, wantSHA256)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(configJSON) == "" {
		configJSON = "{}"
	}
	var x any
	if err := json.Unmarshal([]byte(configJSON), &x); err != nil {
		return nil, fmt.Errorf("invalid GBase 8a provider config JSON: %w", err)
	}
	cp := C.CString(clean)
	cc := C.CString(configJSON)
	defer C.free(unsafe.Pointer(cp))
	defer C.free(unsafe.Pointer(cc))
	var ce *C.char
	p := C.qm_load(cp, cc, &ce)
	if ce != nil {
		defer C.free(unsafe.Pointer(ce))
	}
	if p == nil {
		if ce != nil {
			return nil, errors.New(C.GoString(ce))
		}
		return nil, errors.New("failed to load GBase 8a CDC native provider")
	}
	return &nativeProvider{ptr: p}, nil
}
func (p *nativeProvider) Health(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr == nil {
		return errors.New("GBase 8a CDC provider is closed")
	}
	var ce *C.char
	rc := C.qm_health(p.ptr, &ce)
	if ce != nil {
		defer C.free(unsafe.Pointer(ce))
	}
	if rc != 0 {
		if ce != nil {
			return errors.New(C.GoString(ce))
		}
		return fmt.Errorf("provider health rc=%d", int(rc))
	}
	return nil
}
func (p *nativeProvider) call(which int, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	cr := C.CString(string(b))
	defer C.free(unsafe.Pointer(cr))
	var ce *C.char
	resp := C.qm_json(p.ptr, C.int(which), cr, &ce)
	if ce != nil {
		defer C.free(unsafe.Pointer(ce))
	}
	if resp == nil {
		if ce != nil {
			return errors.New(C.GoString(ce))
		}
		return errors.New("provider returned no response")
	}
	defer C.free(unsafe.Pointer(resp))
	return json.Unmarshal([]byte(C.GoString(resp)), out)
}
func (p *nativeProvider) Checkpoint(_ context.Context, r CheckpointRequest) (*CheckpointResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out CheckpointResponse
	err := p.call(1, r, &out)
	return &out, err
}
func (p *nativeProvider) Read(_ context.Context, r ReadRequest) (*ReadResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out ReadResponse
	err := p.call(2, r, &out)
	return &out, err
}
func (p *nativeProvider) Ack(_ context.Context, r AckRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	cr := C.CString(string(b))
	defer C.free(unsafe.Pointer(cr))
	var ce *C.char
	rc := C.qm_ack(p.ptr, cr, &ce)
	if ce != nil {
		defer C.free(unsafe.Pointer(ce))
	}
	if rc != 0 {
		if ce != nil {
			return errors.New(C.GoString(ce))
		}
		return fmt.Errorf("provider ack rc=%d", int(rc))
	}
	return nil
}
func (p *nativeProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ptr != nil {
		C.qm_close(p.ptr)
		p.ptr = nil
	}
	return nil
}
