import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.BatchUpdateStatement
import org.jetbrains.exposed.sql.transactions.transaction
import org.testcontainers.containers.PostgreSQLContainer
import java.time.LocalDateTime
import javax.sql.DataSource
import kotlin.random.Random

const val ROWS_COUNT = 4000
const val DATA_FIELD_SIZE = 1600
const val UPDATE_TRX_NUMBER = 100
const val UPDATE_ROWS_NUMBER = 1000

fun main(): Unit = LocalPostgres().use { pg ->
    val ids = (1..ROWS_COUNT).map { Random.nextLong() }

    initData(ids)
    printStats()

    repeat(10) {
        makeUpdates(ids)
        printStats()
    }

    vacuum(pg.dataSource)

    repeat(10) {
        makeUpdates(ids)
        printStats()
    }
}

private fun initData(ids: List<Long>) {
    val alphabet = "abcdefghigklmnopqrstuvwxwzABCDEFGHIGKLMNOPQRSTUVWXWZ0123456789"

    transaction {
        SchemaUtils.create(TestTable)
        TestTable.batchInsert(ids) {
            this[TestTable.id] = it
            this[TestTable.data] = buildString {
                repeat(DATA_FIELD_SIZE) { append(alphabet[Random.nextInt(alphabet.length)]) }
            }
            this[TestTable.rnd] = Random.nextLong()
        }
    }
}

private fun makeUpdates(ids: List<Long>) {
    repeat(UPDATE_TRX_NUMBER) {
        transaction {
            BatchUpdateStatement(TestTable).apply {
                ids.shuffled().take(UPDATE_ROWS_NUMBER).forEach {
                    addBatch(EntityID(it, TestTable))
                    this[TestTable.rnd] = Random.nextLong()
                }
                execute(this@transaction)
            }
        }
    }
}

private fun printStats() {
    println("=== ${LocalDateTime.now()} ===")

    transaction {
        val rowsCnt = TestTable.selectAll().count()
        val tableSize = exec("SELECT pg_size_pretty( pg_total_relation_size('${TestTable.tableName}') )") { rs ->
            rs.next()
            rs.getString(1)
        }
        val stats = exec(
            """
            select n_live_tup, n_dead_tup, vacuum_count, autovacuum_count 
              from pg_stat_all_tables 
             where relname='${TestTable.tableName}'
             """
        ) { rs ->
            rs.next()
            TableStats(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4))
        }!!
        println("Stats: rows_cnt: $rowsCnt, table_size: $tableSize, live tuples: ${stats.liveTuples}, dead tuples: ${stats.deadTuples}, vacuum_cnt: ${stats.vacuumCnt}, autovacuum_cnt: ${stats.autoVacuumCnt}")
    }

    printFullScanEla()
}

private fun printFullScanEla() {
    transaction {
        val minEla = (1..10).map {
            val ts = System.nanoTime()
            exec("SELECT sum(rnd) from ${TestTable.tableName}") { rs ->
                rs.next()
            }
            System.nanoTime() - ts
        }.min()
        println(String.format("> fullscan ela: \u001b[31m%.3f\u001b[0mms", minEla / 1_000_000.0))
    }
}

private fun vacuum(ds: DataSource) {
    println("*** VACUUM ***")
    ds.connection.use { conn ->
        conn.createStatement().use { stm ->
            stm.execute("VACUUM FULL ANALYZE ${TestTable.tableName}")
        }
    }
    Thread.sleep(10_000)
}

class LocalPostgres() : AutoCloseable {
    val postgresContainer = PostgreSQLContainer("postgres:14").apply { start() }

    val dataSource = HikariDataSource().apply {
        jdbcUrl = postgresContainer.jdbcUrl
        username = postgresContainer.username
        password = postgresContainer.password
    }

    val exposed = Database.connect(dataSource)

    override fun close() = dataSource.close()
        .also { postgresContainer.close() }
}

object TestTable : LongIdTable("test_table") {
    val data = mediumText("data")
    val rnd = long("rnd")
}

data class TableStats(
    val liveTuples: Long, val deadTuples: Long, val vacuumCnt: Long, val autoVacuumCnt: Long
)