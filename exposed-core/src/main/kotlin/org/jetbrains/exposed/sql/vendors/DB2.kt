package org.jetbrains.exposed.sql.vendors

import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.exceptions.UnsupportedByDialectException
import org.jetbrains.exposed.exceptions.throwUnsupportedException
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager

internal object DB2DataTypeProvider : DataTypeProvider() {

    override fun binaryType(): String {
        exposedLogger.error("The length of the Binary column is missing.")
        error("The length of the Binary column is missing.")
    }

    override fun byteType(): String = "SMALLINT"

    override fun ubyteType(): String = "SMALLINT"

    override fun dateTimeType(): String = "TIMESTAMP"

    override fun ulongType(): String = "BIGINT"

    override fun textType(): String = "VARCHAR(32704)"

    override fun integerAutoincType(): String = "INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"

    override fun longAutoincType(): String = "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"
}

internal object DB2FunctionProvider : FunctionProvider() {

    override fun random(seed: Int?) = "RAND(${seed?.toString().orEmpty()})"

    override fun <T : String?> groupConcat(
        expr: GroupConcat<T>,
        queryBuilder: QueryBuilder
    ): Unit = queryBuilder {
        if (expr.orderBy.size != 1) {
            TransactionManager.current().throwUnsupportedException("SQLServer supports only single column in ORDER BY clause in LISTAGG")
        }
        append("LISTAGG(")
        append(expr.expr)
        expr.separator?.let {
            append(", '$it'")
        }
        append(") WITHIN GROUP (ORDER BY ")
        val (col, order) = expr.orderBy.single()
        append(col, " ", order.name, ")")
    }
}

/**
 * DB2 dialect implementation.
 */
class DB2Dialect : VendorDialect(dialectName, DB2DataTypeProvider, DB2FunctionProvider) {
    override val name: String = dialectName
    override val supportsOnlyIdentifiersInGeneratedKeys: Boolean = true
    override val supportsIfNotExists: Boolean = false

    override fun createDatabase(name: String): String {
        throw UnsupportedByDialectException("Create database can only run in clp(command line processor), thus it can not run here", currentDialect)
    }

    override fun dropDatabase(name: String): String {
        throw UnsupportedByDialectException("Drop database can only run in clp(command line processor), thus it can not run here", currentDialect)
    }

    override fun createSchema(schema: Schema): String = buildString {
        append("CREATE SCHEMA ")
        append(schema.identifier)

        if (schema.authorization != null) {
            append(" ")
            append("AUTHORIZATION ")
            append(schema.authorization)
        }
    }

    override fun createIndex(index: Index): String {
        return super.createIndex(index)
    }




    override fun dropSchema(schema: Schema, cascade: Boolean): String {
        if (cascade) {
            throw UnsupportedByDialectException(
                "${currentDialect.name} There is no cascading drop function in DB2; you will have to drop each individual object that uses that schema first",
                currentDialect
            )
        }
        return "DROP SCHEMA ${schema.identifier}"
    }

    companion object {
        /** DB2 dialect name */
        const val dialectName: String = "db2"
    }
}
