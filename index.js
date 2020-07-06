#!/usr/bin/env node

const axios = require('axios')
const H = require('highland')
const parse = require('csv-parse')

const { db, addAnnotation, random } = require('../database/google-cloud')

const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT16cUw087uaTe3XKQw-pYUMw-gHAvs63dareFbvO8Eo-r7Go9YpPsOLfaXRq-uss0GnMyH1uIIt6xX/pub?output=csv'
const city = 'amsterdam'

const parser = parse({
  delimiter: ',',
  columns: true
})

async function addPoi (row) {
  if (!row.kvk) {
    console.error('Invalid row:', row)
    return
  }

  const kvkId = row.kvk.trim()
  const poiId = `faillissementsdossier:${kvkId}`

  const addresses = [
    row.adres0,
    row.adres1,
    row.adres2,
    row.adres3,
    row.adres4,
    row.adres5
  ].filter((d) => d).map((address) => {
    if (address.startsWith('Correspondentieadres: ')) {
      return {
        type: 'correspondentieadres',
        address: address.replace('Correspondentieadres: ', '')
      }
    } else if (address.startsWith('Vestigingsadres: ')) {
      return {
        type: 'vestigingsadres',
        address: address.replace('Vestigingsadres: ', '')
      }
    } else if (address.startsWith('Woonadres: ')) {
      return {
        type: 'woonadres',
        address: address.replace('Woonadres: ', '')
      }
    } else {
      throw new Error(`Unknown address type: ${address}`)
    }
  })

  const data = {
    name: row.Bedrijfsnaam,
    url: row.Link,
    date: row.Datum,
    kvkId,
    groep: row.groep,
    hoofdactiviteit: row.hoofdactiviteit,
    nevenactiviteiten: [
      row['nevenactiviteit 1'],
      row['nevenactiviteit 2']
    ].filter((d) => d),
    addresses
  }

  const poiRef = db.collection('pois').doc(poiId)
  const poi = await poiRef.get()

  if (!poi.exists) {
    await poiRef.set({
      city,
      source: 'faillissementsdossier',
      url: data.url,
      random: random()
    })

    await addAnnotation(poiId, 'faillissementsdossier', data)

    const screenshotAddresses = data.addresses.filter((address) => address.type === 'vestigingsadres')
    if (screenshotAddresses.length) {
      for (let address of screenshotAddresses) {
        await addAnnotation(poiId, 'address', {
          address: address.address
        })
      }
    }
  }
}

async function run () {
  const response = await axios({
    method: 'get',
    url: csvUrl,
    responseType: 'stream'
  })

  const rows = response.data.pipe(parser)
  H(rows)
    .flatMap((row) => H(addPoi(row)))
    .done(() => {
      console.log('Done!')
    })
}

run()
