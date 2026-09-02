import { createRouter, createWebHistory } from 'vue-router'
import MainLayout from '../layout/MainLayout.vue'
import Login from '../views/Login.vue'
import Dashboard from '../views/Dashboard.vue'
import Datasources from '../views/Datasources.vue'
import Migrations from '../views/Migrations.vue'
import Workers from '../views/Workers.vue'
import Alerts from '../views/Alerts.vue'
import Audit from '../views/Audit.vue'
import Settings from '../views/Settings.vue'
import Users from '../views/Users.vue'
import ValidationCenter from '../views/ValidationCenter.vue'
import CutoverCenter from '../views/CutoverCenter.vue'
import MonitoringCenter from '../views/MonitoringCenter.vue'

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
