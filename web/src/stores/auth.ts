import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, ApiError, browserSessionChanged, credentialRevision } from '../api/client'

export type Identity = {username:string;role:string;authenticated:boolean;open_mode?:boolean;personal_tokens_enabled?:boolean}

export const useAuthStore = defineStore('auth',()=>{
  const identity=ref<Identity|null>(null)
  const checked=ref(false)

  async function refresh(){
    const version=credentialRevision()
    try{
      const current=await api<Identity>('/api/v1/auth/me')
      if(version!==credentialRevision())return false
      identity.value=current
      checked.value=true
      return true
    }catch(error){
      if(version!==credentialRevision())return false
      if(error instanceof ApiError&&error.status===401){identity.value=null;checked.value=true;return false}
      // Network and server errors are retryable; do not destroy valid tokens.
      throw error
    }
  }

  function invalidate(){identity.value=null;checked.value=false}

  async function logout(){
    await api('/api/v1/auth/logout', {method:'POST'})
    browserSessionChanged()
    identity.value=null
    checked.value=true
  }

  return {identity,checked,refresh,logout,invalidate}
})
