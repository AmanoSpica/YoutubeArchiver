const { createConnection } = require('mysql2')
const mysql = require('mysql2/promise')
let client

const createConnection = async() => {
    client = await mysql.createConnection({
        host: "",
        port: 3306,
        user: "",
        password: "",
        database: ""
    })
}

const updateCron = async() => {
    await createConnection()
    const [result, fields] = await client.query(
        "UPDATE QuotaData SET quota = 0;"
    )
    await client.end()
    console.log(result)
}