<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { api } from '../api/client'

type User={id:string;username:string;role:string;enabled:boolean;created_at:string;updated_at:string;last_login_at?:string}
const users=ref<User[]>([]),dialog=ref(false),edit=ref<User|null>(null),passwordDialog=ref(false),passwordUser=ref<User|null>(null)
const form=reactive({username:'',password:'',role:'viewer',enabled:true})
const password=ref('')
async function load(){try{users.value=await api<User[]>('/api/v1/users')}catch(e:any){ElMessage.error(e.message)}}
function openCreate(){edit.value=null;Object.assign(form,{username:'',password:'',role:'viewer',enabled:true});dialog.value=true}
function openEdit(u:User){edit.value=u;Object.assign(form,{username:u.username,password:'',role:u.role,enabled:u.enabled});dialog.value=true}
async function save(){try{if(edit.value){await api(`/api/v1/users/${edit.value.id}`,{method:'PUT',body:JSON.stringify({username:form.username,role:form.role,enabled:form.enabled})})}else{await api('/api/v1/users',{method:'POST',body:JSON.stringify(form)})};dialog.value=false;ElMessage.success('已保存');await load()}catch(e:any){ElMessage.error(e.message)}}
function openPassword(u:User){passwordUser.value=u;password.value='';passwordDialog.value=true}
async function resetPassword(){if(!passwordUser.value)return;try{await api(`/api/v1/users/${passwordUser.value.id}/password`,{method:'POST',body:JSON.stringify({password:password.value})});passwordDialog.value=false;ElMessage.success('密码已重置')}catch(e:any){ElMessage.error(e.message)}}
async function toggle(u:User){try{await ElMessageBox.confirm(`${u.enabled?'禁用':'启用'}用户 ${u.username}？`,'确认');await api(`/api/v1/users/${u.id}`,{method:'PUT',body:JSON.stringify({enabled:!u.enabled})});await load()}catch(e:any){if(e!=='cancel')ElMessage.error(e.message)}}
onMounted(load)
</script>
<template><div>
  <div class="page-title"><div><h1>用户与权限</h1><p>QMigration 账号采用 Admin / DBA / Operator / Viewer 四级 RBAC。</p></div><el-button type="primary" @click="openCreate">创建用户</el-button></div>
  <div class="panel"><el-table :data="users"><el-table-column prop="username" label="用户名" min-width="140"/><el-table-column label="角色" width="120"><template #default="s"><el-tag>{{s.row.role}}</el-tag></template></el-table-column><el-table-column label="状态" width="100"><template #default="s"><el-tag :type="s.row.enabled?'success':'info'">{{s.row.enabled?'启用':'禁用'}}</el-tag></template></el-table-column><el-table-column prop="last_login_at" label="最后登录" min-width="190"/><el-table-column prop="created_at" label="创建时间" min-width="190"/><el-table-column label="操作" width="260"><template #default="s"><el-button size="small" @click="openEdit(s.row)">编辑</el-button><el-button size="small" @click="openPassword(s.row)">重置密码</el-button><el-button size="small" :type="s.row.enabled?'danger':'success'" plain @click="toggle(s.row)">{{s.row.enabled?'禁用':'启用'}}</el-button></template></el-table-column></el-table></div>
  <el-dialog v-model="dialog" :title="edit?'编辑用户':'创建用户'" width="520"><el-form label-width="90"><el-form-item label="用户名"><el-input v-model="form.username"/></el-form-item><el-form-item v-if="!edit" label="密码"><el-input v-model="form.password" type="password" show-password/></el-form-item><el-form-item label="角色"><el-select v-model="form.role" style="width:100%"><el-option label="Admin" value="admin"/><el-option label="DBA" value="dba"/><el-option label="Operator" value="operator"/><el-option label="Viewer" value="viewer"/></el-select></el-form-item><el-form-item label="启用"><el-switch v-model="form.enabled"/></el-form-item></el-form><template #footer><el-button @click="dialog=false">取消</el-button><el-button type="primary" @click="save">保存</el-button></template></el-dialog>
  <el-dialog v-model="passwordDialog" title="重置密码" width="480"><el-input v-model="password" type="password" show-password placeholder="至少 8 个字符"/><template #footer><el-button @click="passwordDialog=false">取消</el-button><el-button type="primary" @click="resetPassword">重置</el-button></template></el-dialog>
</div></template>
