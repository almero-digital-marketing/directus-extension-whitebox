const { machineIdSync } = require('node-machine-id')
const aguid = require('aguid')
const os = require('os')
const { v1: uuidv1 } = require('uuid')
const axios = require('axios')
const Promise = require('bluebird')
const { flockAsync } = Promise.promisifyAll(require('fs-ext'))	
const Queue = require('queue')
const FormData = require('form-data')
const hasha = require('hasha')
const path = require('path')
const glob = require('glob-promise')
const fs = require('fs-extra-promise')
const { debounce } = require('throttle-debounce')
const yaml = require('js-yaml');
const _ = require('lodash')
require('deepdash')(_)

export default async ({ filter, action, init }, options) => {
	let stamp = Date.now()
	const machineId = machineIdSync() + '_' + os.hostname() + '_' + os.userInfo().username
	let queue = Queue({
		concurrency: 3,
		autostart: true
	})
	const uploadsFolder = path.resolve(options.env['STORAGE_LOCAL_ROOT'])
	const defaultFields = [new Array(4).fill('*').join('.')]
	const runtimeFolder = './runtime'
	await fs.emptyDir(runtimeFolder)

	const { ItemsService } = options.services;

	const schemaDump = yaml.dump(await options.getSchema())
	const schemaFile = path.join(runtimeFolder, 'schema.yml')
	await fs.outputFileAsync(schemaFile, schemaDump, 'utf8')

	let clear = Promise.resolve()
	if (options.env.WHITEBOX_CLEAR || options.env.WHITEBOX_REFRESH) {
		console.log('Clear whtiebox data')
		let data = {
			context: options.env['WHITEBOX_GLOBAL'] != 'local' ? 'shared' : machineId,
		}
		clear = api('feed', '/api/catalog/clear', data)
		.then(() => {
			if (options.clear) {
				return axios.post(options.env['WHITEBOX_STORAGE_URL'] + '/' + options.env['WHITEBOX_STORAGE_TOKEN'] + '/clear', {})
			}
		})
		.catch((err) => console.error('Error clearing:', err))
	}

	let pendingUploads = {}
	function api(service, url, data) {
		// console.log(JSON.stringify(data, null, 2))
		return axios
			.post(options.env['WHITEBOX_' + service.toUpperCase() + '_URL'] + url + '?v=' + Date.now(), data, {
				headers: {
					Authorization: 'Bearer ' + options.env['WHITEBOX_' + service.toUpperCase() + '_TOKEN'],
				},
			})
			.then((response) => {
				if (response.data.success) {
					return Promise.resolve(response.data)
				} else {
					console.error('Api service error:', url, data, response.data.message)
					return Promise.reject(response.data.message)
				}
			})
			.catch((err) => {
				console.error('Api system error:', url, data, err)
				return Promise.resolve()
			})
	}

	function upload(file) {
		if (pendingUploads[file]) return Promise.resolve()
		pendingUploads[file] = true
		return fs.openAsync(file, 'r').then(fd => {
			return flockAsync(fd, 'sh').then(() => {
				// mikser.diagnostics.log(this, 'debug', `[whitebox] File locked: ${file}`)
				let relative = file.replace(uploadsFolder, '')
				let data = {
					file: relative,
					context: options.env['WHITEBOX_GLOBAL'] != 'local' ? 'shared' : machineId,
				}
				return axios
				.post(options.env['WHITEBOX_STORAGE_URL'] + '/' + options.env['WHITEBOX_STORAGE_TOKEN'] + '/hash', data)
				.then((response) => {
					return hasha.fromFile(file, { algorithm: 'md5' }).then((hash) => {
							// mikser.diagnostics.log(this, 'debug', `[whitebox] MD5: ${file} ${hash} ${response.data.hash}`)
							if (!response.data.success || hash != response.data.hash) {
								let uploadHeaders = {
									context: options.env['WHITEBOX_GLOBAL'] != 'local' ? 'shared' : machineId,
								}
								if (options.env['WHITEBOX_GLOBAL'] != 'local') {
									uploadHeaders.expire = options.env['WHITEBOX_EXPIRE'] || '10 days'
								}
								let form = new FormData()
								form.append(relative, fs.createReadStream(file))
								let formHeaders = form.getHeaders()
								return axios
								.post(options.env['WHITEBOX_STORAGE_URL'] + '/upload', form, {
									headers: {
										Authorization: 'Bearer ' + options.env['WHITEBOX_STORAGE_TOKEN'],
										...formHeaders,
										...uploadHeaders,
									},
									maxContentLength: Infinity,
									maxBodyLength: Infinity
								})
								.then((response) => {
									if (response.data.uploads) {
										for (let file in response.data.uploads) {
											console.log(
												'📦 ', file, 
											)
											console.log(
												'🌐 ', response.data.uploads[file]
											)
										}
									}
								})
								.catch((err) => console.error('Error uplaoding:', err))									
							}
						})
					})
					.then(() => {
						return flockAsync(fd, 'un')
					})
					.catch(err => {
						console.error(err)
						return flockAsync(fd, 'un')
					})
			}).catch(err => {
				console.error('Lock failed:', file, err)
			})
		}).then(() => delete pendingUploads[file])
	}

	function dataItem(collection, item, schema) {
		const refId = item.href || ('/' + collection + '/' + item[schema.collections[collection].primary])
		const meta = _.mapKeysDeep(item, (value, key) => _.camelCase(key))
		meta.href = meta.href || refId
		const data = {
			passportId: uuidv1(),
			date: item.date_updated || item.data_created,
			vaultId: aguid(refId),
			refId: item.url || refId,
			type: 'directus.' + item.layout,
			context: options.env['WHITEBOX_GLOBAL'] != 'local' ? 'shared' : machineId,
			data: {
				stamp,
				importDate: new Date(),
				meta
			}
		}
		if (options.env['WHITEBOX_GLOBAL'] != 'local') {
			data.expire = options.env.WHITEBOX_EXPIRE || '10days'
		}
		if (options.env['WHITEBOX_VAULTS']) {
			data.vaults = options.env['WHITEBOX_VAULTS']
		}
		return data
	}

	async function syncFilesAsync() {
		if (!options.clear) {
			let files = await glob('**/*', { cwd: uploadsFolder })
			for (let file of files) {
				file = path.join(uploadsFolder, file)
				const stat = await fs.lstatAsync(file)
				if (stat.isFile()) {
					await upload(file)
				}
			}
		}
	}
	const syncFiles = debounce(1000, syncFilesAsync)

	let documentsSyncStatus = {}
	let layouts = []
	async function syncDocumentsAsync() {
		const schema = await options.getSchema()
		let { collections } = schema
		const documentCollections = Object
		.keys(collections)
		.filter(key => key.indexOf('directus_') != 0)
		.map(key => collections[key])
		.filter(collection => {
			return collection.fields.target && collection.fields.target.defaultValue == 'whitebox'
		})
	
		console.log('Document collections:', documentCollections.map(documentCollection => documentCollection.collection).join(', '))
		for(let documentCollection of documentCollections) {
			const itemsService = new ItemsService(documentCollection.collection, { schema })
			const fields = schema.collections[documentCollection.collection].fields.fields?.defaultValue?.split(',').map(field => field.trim()) || defaultFields
			const items = await itemsService.readByQuery({ 
				fields,
				limit: -1
			})
			console.log(`Collection [${documentCollection.collection}]:`, items.length, fields)
			for(let item of items) {
				if (item.target != 'whitebox' || !item.layout) continue
				if (layouts.indexOf(item.layout) == -1) layouts.push(item.layout)
	
				if (!options.env['WHITEBOX_CLEAR']) {
					const data = dataItem(documentCollection.collection, item, schema)
	
					let checksum = await hasha.async(JSON.stringify(data.data.meta))
					if (documentsSyncStatus[data.refId] != checksum) {
						documentsSyncStatus[data.refId] = checksum
	
						const documentDump = yaml.dump(data.data.meta)
						const documentFile = path.join(runtimeFolder, data.refId + '.yml')
						await fs.outputFileAsync(documentFile, documentDump, 'utf8')
				
						queue.push(() => {
							console.log('Import document:', documentFile)
							return api('feed', '/api/catalog/keep/one', data)
						})
					}
	
				}
			}
		}
	}
	const syncDocuments = debounce(1000, syncDocumentsAsync)

	async function expireDocumentsAsync() {
		documentsSyncStatus = {}
		stamp = Date.now()
		layouts = []
	
		await syncDocumentsAsync()
	
		queue.push(async () => {
			console.log('Layouts:', layouts.join(', '))
			for(let layout of layouts) {
				console.log('Expire layout:', layout)
				await api('feed', '/api/catalog/expire', {
					type: 'directus.' + layout,
					stamp
				})
			}
		})
	}
	const expireDocuments = debounce(1000, expireDocumentsAsync)

	action('items.create', syncDocuments)
	action('items.update', syncDocuments)
	action('items.delete', expireDocuments)

	init('app.after', async () => {
		expireDocuments()
		syncFiles()
		console.log('WhiteBox sync started!');
	})

	queue.on('success', debounce(1000, () => {
		if (!options.env['WHITEBOX_CLEAR']) {
			console.log('Clear cache')
			let data = {
				context: options.env['WHITEBOX_GLOBAL'] != 'local' ? 'shared' : machineId
			}
			return api('feed', '/api/catalog/clear/cache', data)
		}
	}))
}
