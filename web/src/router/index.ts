import { createRouter, createWebHistory } from 'vue-router'
import MainLayout from '../layout/MainLayout.vue'

const Login = () => import('../views/Login.vue')
const Dashboard = () => import('../views/Dashboard.vue')
const Datasources = () => import('../views/Datasources.vue')
const Migrations = () => import('../views/Migrations.vue')
const Workers = () => import('../views/Workers.vue')
const Alerts = () => import('../views/Alerts.vue')
const Audit = () => import('../views/Audit.vue')
const Settings = () => import('../views/Settings.vue')
const Users = () => import('../views/Users.vue')
const ValidationCenter = () => import('../views/ValidationCenter.vue')
const CutoverCenter = () => import('../views/CutoverCenter.vue')
const MonitoringCenter = () => import('../views/MonitoringCenter.vue')

export default createRouter({
  history:createWebHistory(),
  routes:[
    {path:'/login',component:Login},
    {path:'/',component:MainLayout,children:[
      {path:'',component:Dashboard},
      {path:'datasources',component:Datasources},
      {path:'migrations',component:Migrations},
      {path:'validation',component:ValidationCenter},
      {path:'cutover',component:CutoverCenter},
      {path:'monitoring',component:MonitoringCenter},
      {path:'workers',component:Workers},
      {path:'alerts',component:Alerts},
      {path:'audit',component:Audit},
      {path:'users',component:Users},
      {path:'settings',component:Settings},
    ]}
  ]
})
