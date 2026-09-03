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

const router=createRouter({
  history:createWebHistory(),
  routes:[
    {path:'/login',component:Login,meta:{public:true,title:'登录'}},
    {path:'/',component:MainLayout,children:[
      {path:'',component:Dashboard,meta:{title:'运行概览'}},
      {path:'datasources',component:Datasources,meta:{title:'数据源'}},
      {path:'migrations',component:Migrations,meta:{title:'迁移任务'}},
      {path:'validation',component:ValidationCenter,meta:{title:'校验中心'}},
      {path:'cutover',component:CutoverCenter,meta:{title:'割接中心'}},
      {path:'monitoring',component:MonitoringCenter,meta:{title:'监控中心'}},
      {path:'workers',component:Workers,meta:{title:'Worker 节点'}},
      {path:'alerts',component:Alerts,meta:{title:'告警中心'}},
      {path:'audit',component:Audit,meta:{title:'操作审计'}},
      {path:'users',component:Users,meta:{title:'用户与权限',requiresAdmin:true}},
      {path:'settings',component:Settings,meta:{title:'访问设置'}},
    ]},
    {path:'/:pathMatch(.*)*',redirect:'/'},
  ]
})

router.afterEach(to=>{document.title=`${String(to.meta.title||'控制台')} · QMigration`})
export default router
