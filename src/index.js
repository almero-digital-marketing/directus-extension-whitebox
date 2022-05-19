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
const { throttle } = require('throttle-debounce')

export default async ({ filter, action, init }, options) => {
	// console.log(options)
	const stamp = Date.now()
	const machineId = machineIdSync() + '_' + os.hostname() + '_' + os.userInfo().username
	let queue = Queue({
		concurrency: 3,
		autostart: true
	})
	const uploadsFolder = path.resolve(options.env['STORAGE_LOCAL_ROOT'])
	const changes = new Set()
	const itemDepth = [new Array(4).fill('*').join('.')]

	const { ItemsService } = options.services;
	const schema = await options.getSchema()
	let { collections } = schema
	const documentCollections = Object
	.keys(collections)
	.filter(key => key.indexOf('directus_') != 0)
	.map(key => collections[key])
	.filter(collection => {
		return collection.fields.target && collection.fields.target.defaultValue == 'whitebox'
	})

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
										context: machineId
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
		const data = {
			passportId: uuidv1(),
			date: item.date_updated || item.data_created,
			vaultId: aguid(refId),
			refId: item.url || refId,
			type: 'directus.' + item.layout,
			data: {
				stamp,
				importDate: new Date(),
				meta: item
			}
		}
		if (!options.env['WHITEBOX_GLOBAL']) {
			data.context = machineId
			data.expire = options.env.WHITEBOX_EXPIRE || '10days'
		}
		return data
	}

	const clearCache = throttle(1000, () => {
		if (!options.env['WHITEBOX_CLEAR']) {
			queue.push(() => {
				console.log('Clear cache')
				let data = {}
				if (!options.env['WHITEBOX_GLOBAL']) data.context = machineId
				return api('feed', '/api/catalog/clear/cache', data)
			})
		}
	})

	async function importDocument({key, collection}, context) {
		const itemsService = new ItemsService(collection, {
			schema: context.schema,
		})
		let item = await itemsService.readOne(key, { 
			fields: itemDepth,
		})

		if (item.target != 'whitebox' || !item.layout) return
		if (!options.env['WHITEBOX_CLEAR']) {
			const data = dataItem(collection, item)
			queue.push(() => {
				console.log('Document imported:', data.refId)
				return api('feed', '/api/catalog/keep/one', data)
			})
		}
	}
	
	function deleteDocument({key, collection}, context) {
		const refId = '/' + collection + '/' + key
		const vaultId = aguid(refId)
		if (!options.env['WHITEBOX_CLEAR']) {
			queue.push(() => {
				console.log('Document deleted:', refId)
				return api('feed', '/api/catalog/remove', { vaultId })
			})
		}
	}

	async function sync() {
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

	async function followRelations({key, collection}, context) {
		const id = `/${collection}/${key}`
		if (changes.has(id)) return
		changes.add(id)

		const related = context.schema.relations.filter(relation => relation.collection == collection)
		for (let relation of related) {
			const itemsService = new ItemsService(collection, {
				schema: context.schema,
			})
			const item = await itemsService.readOne(key, { fields: ['*'] })
			if (relation.related_collection != null) {
				await changeRelatedItems({ key: item[relation.field], collection: relation.related_collection }, context)
			} 
			if (relation.meta.one_allowed_collections != null) {
				for(let oneCollection of relation.meta.one_allowed_collections) {
					await changeRelatedItems({ key: item[relation.field], collection: oneCollection }, context)
				}
			}
		}
	}

	async function changeRelatedItems({key, collection}, context) {
		const id = `/${collection}/${key}`
		if (changes.has(id)) return
		changes.add(id)

		const related = context.schema.relations
		.filter(relation => (relation.related_collection && relation.related_collection == collection) || (relation.meta.one_allowed_collections && relation.meta.one_allowed_collections.indexOf(collection) > -1))

        for (let relation of related) {
			const itemsService = new ItemsService(relation.collection, {
				schema: context.schema,
			})
			if (context.schema.collections[relation.collection].fields[relation.field].type == 'integer') {
				key = Number(key)
			}
			const filter = { [relation.field]: key }
            const items = await itemsService.readByQuery({ filter })
			for (let item of items) {
				await followRelations({ key: item.id, collection: relation.collection }, context)
			}
        }
    }

	async function processChanges(context) {
		if (changes.size > 1) {
			let ids = [...changes].slice(1)
			console.log('Changes:', ids.join(', '))
			for (let id of ids) {
				const [,relatedCollection, relatedKey] = id.split('/')
				await importDocument({ key: relatedKey, collection: relatedCollection }, context)
			}
		}
		changes.clear()
	}

	action('items.create', async (meta, context) => {
		console.log('Items create action')
		try {
			await importDocument(meta, context)
			clearCache()
		} catch (e) {
			console.error('Items create action failed', e)
		}
	})
	filter('items.update', async (payload, meta, context) => {
		try {
			for(let key of meta.keys) {
				console.log('Items update filter:', meta.collection, key)
				await changeRelatedItems({key, collection: meta.collection}, context)
			}
		} catch (e) {
			console.error('Items update filter failed', e)
		}
	})
	action('items.update', async (meta, context) => {
		try {
			for(let key of meta.keys) {
				console.log('Items update action:', meta.collection, key)
				await importDocument({key, collection: meta.collection}, context)
				await changeRelatedItems({key, collection: meta.collection}, context)
			}
			await processChanges(context)
			clearCache()
		} catch (e) {
			console.error('Items update action failed', meta.collection, key, e)
		}
	})
	filter('items.delete', async (keys, { collection }, context) => {
		try {
			for(let key of keys) {
				console.log('Items delete filter:', collection, key)
				await changeRelatedItems({key, collection, changes}, context)
			}
			await processChanges(context)
			clearCache()
		} catch (e) {
			console.error('Items delete filter failed', e)
		}
	})
	action('items.delete', async (meta, context) => {
		try {
			for(let key of meta.payload) {
				console.log('Items delete action:', meta.collection, key)
				await deleteDocument({key, collection: meta.collection}, context)
			}
			await processChanges(context)
			clearCache()
		} catch (e) {
			console.error('Items delete action failed', e)
		}
	})
	action('files.upload', sync)
	
	init('app.after', async () => {
		console.log('Document collections:', documentCollections.length)
		for(let documentCollection of documentCollections) {
			const itemsService = new ItemsService(documentCollection.collection, {
				schema,
			})

			const items = await itemsService.readByQuery({ 
				fields: itemDepth 
			})
			const layouts = []
			console.log(`Collection [${documentCollection.collection}]:`, items.length)
			for(let item of items) {
				if (item.target != 'whitebox' || !item.layout) continue
				if (layouts.indexOf(item.layout) == -1) layouts.push(item.layout)
				if (!options.env['WHITEBOX_CLEAR']) {
					const data = dataItem(documentCollection.collection, item)
					queue.push(() => {
						console.log('Document imported:', data.refId)
						return api('feed', '/api/catalog/keep/one', data)
					})
				}
			}
			
			console.log('Layouts:', layouts.join(', '))
			for(let layout of layouts) {
				queue.push(() => {
					return api('feed', '/api/catalog/expire', {
						type: 'directus.' + layout,
						stamp
					})
				}) 
			}
		}
		clearCache()
		await sync()
		console.log('Application started!');
	})
}
