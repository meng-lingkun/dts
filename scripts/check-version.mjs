import {readFileSync, readdirSync} from 'node:fs'
import {join} from 'node:path'

const read = path => readFileSync(path, 'utf8')
const version = read('VERSION').trim()
const failures = []

const backend = read('backend/internal/version/version.go').match(/Version\s*=\s*"([^"]+)"/)?.[1]
if (backend !== version) failures.push(`backend version ${backend ?? '<missing>'} != ${version}`)

const web = JSON.parse(read('web/package.json')).version
if (web !== version) failures.push(`web package version ${web} != ${version}`)

for (const manifest of readdirSync('deployments/kubernetes').filter(name => name.endsWith('.yaml'))) {
  const kubernetes = read(join('deployments/kubernetes', manifest))
  for (const match of kubernetes.matchAll(/image:\s+qmigration\/(?:server|web):([^\s]+)/g)) {
    if (match[1] !== version) failures.push(`Kubernetes image tag in ${manifest} ${match[1]} != ${version}`)
  }
}

const migrations = readdirSync('backend/migrations').filter(name => name.endsWith('.sql')).sort()
const latestMigration = read(join('backend/migrations', migrations.at(-1)))
if (!latestMigration.includes(`'${version}'`)) failures.push(`latest migration does not mark ${version}`)

if (!read('README.md').includes(`\`${version}\``)) failures.push(`README does not identify ${version}`)

if (failures.length) {
  console.error(failures.join('\n'))
  process.exit(1)
}
console.log(`version consistency ok: ${version}`)
