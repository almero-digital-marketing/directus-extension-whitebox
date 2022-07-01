import { async } from 'hasha'

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
	const itemDepth = [new Array(4).fill('*').join('.')]
	const runtimeFolder = path.join(options.env['INIT_CWD'], 'runtime')
	await fs.emptyDir(runtimeFolder)

	const { ItemsService } = options.services;
	// console.log(JSON.stringify(schema, null, 2))
	
	let clear = Promise.resolve()
	if (options.env.WHITEBOX_CLEAR || options.env.WHITEBOX_REFRESH) {
		console.log('Clear whtiebox data')
		let data = {}
		if (!options.env['WHITEBOX_GLOBAL']) data.context = machineId
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
					file: relative
				}
				if (!options.env['WHITEBOX_GLOBAL']) data.context = machineId
				return axios
				.post(options.env['WHITEBOX_STORAGE_URL'] + '/' + options.env['WHITEBOX_STORAGE_TOKEN'] + '/hash', data)
				.then((response) => {
					return hasha.fromFile(file, { algorithm: 'md5' }).then((hash) => {
							// mikser.diagnostics.log(this, 'debug', `[whitebox] MD5: ${file} ${hash} ${response.data.hash}`)
							if (!response.data.success || hash != response.data.hash) {
								let uploadHeaders = {}
								if (!options.env['WHITEBOX_GLOBAL']) {
									uploadHeaders = {
										expire: options.env['WHITEBOX_EXPIRE'] || '10 days',
										context: data.context
									}
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

	function dataItem(collection, item) {
		const refId = '/' + collection + '/' + item.id
		const meta = _.mapKeysDeep(item, (value, key) => _.camelCase(key))
		const data = {
			passportId: uuidv1(),
			date: item.date_updated || item.data_created,
			vaultId: aguid(refId),
			refId: item.url || refId,
			type: 'directus.' + item.layout,
			data: {
				stamp,
				importDate: new Date(),
				meta
			}
		}
		if (!options.env['WHITEBOX_GLOBAL']) {
			data.context = machineId
			data.expire = options.env.WHITEBOX_EXPIRE || '10days'
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
	
		console.log('Document collections:', documentCollections.join(', '))
		for(let documentCollection of documentCollections) {
			const itemsService = new ItemsService(documentCollection.collection, { schema })
	
			const items = await itemsService.readByQuery({ 
				fields: itemDepth,
				limit: -1
			})
			console.log(`Collection [${documentCollection.collection}]:`, items.length)
			for(let item of items) {
				if (item.target != 'whitebox' || !item.layout) continue
				if (layouts.indexOf(item.layout) == -1) layouts.push(item.layout)
	
				if (!options.env['WHITEBOX_CLEAR']) {
					const data = dataItem(documentCollection.collection, item)
	
					let checksum = await hasha.async(JSON.stringify(data.data.meta))
					if (documentsSyncStatus[data.refId] != checksum) {
						documentsSyncStatus[data.refId] = checksum
	
						const document = yaml.dump(data.data.meta)
						const documentFile = path.join(runtimeFolder, data.refId + '.yml')
						await fs.outputFileAsync(documentFile, document, 'utf8')
				
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
			let data = {}
			if (!options.env['WHITEBOX_GLOBAL']) data.context = machineId
			return api('feed', '/api/catalog/clear/cache', data)
		}
	}))
}
