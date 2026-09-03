import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, clearCredentials } from '../api/client'

export type Identity = {username:string;role:string;open_mode?:boolean}

export const useAuthStore = defineStore('auth',()=>{
  const identity=ref<Identity|null>(null)
  const checked=ref(false)

  async function refresh(){
    try{
      identity.value=await api<Identity>('/api/v1/auth/me')
      checked.value=true
      return true
    }catch{
      clearCredentials()
      identity.value=null
      checked.value=true
      return false
    }
  }

  function logout(){
    clearCredentials()
    identity.value=null
    checked.value=true
  }

  return {identity,checked,refresh,logout}
})
