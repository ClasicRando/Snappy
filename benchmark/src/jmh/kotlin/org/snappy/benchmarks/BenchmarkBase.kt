package org.snappy.benchmarks

import java.sql.Connection

open class BenchmarkBase {
    protected var id = 0
    protected val query = """
        select
            Id as id, [Text] as [text], CreationDate as creationDate, LastChangeDate as lastChangeDate,
            Counter1 as counter1, Counter2 as counter2, Counter3 as counter3, Counter4 as counter4,
            Counter5 as counter5, Counter6 as counter6, Counter7 as counter7, Counter8 as counter8,
            Counter9 as counter9
        from Posts
        where Id = ?
    """.trimIndent()

    val connection: Connection = getConnection()

    protected fun step() {
        id++
        if (id > 5000) id = 1
    }

    protected fun setup() {
        val query = """
            If (Object_Id('Posts') Is Null)
            Begin
            	Create Table Posts
            	(
            		Id int identity primary key,
            		[Text] varchar(max) not null,
            		CreationDate datetime not null,
            		LastChangeDate datetime not null,
            		Counter1 int,
            		Counter2 int,
            		Counter3 int,
            		Counter4 int,
            		Counter5 int,
            		Counter6 int,
            		Counter7 int,
            		Counter8 int,
            		Counter9 int
            	);

            	Set NoCount On;
            	Declare @i int = 0;

            	While @i <= 5001
            	Begin
            		Insert Posts ([Text],CreationDate, LastChangeDate) values (replicate('x', 2000), GETDATE(), GETDATE());
            		Set @i = @i + 1;
            	End
            End
        """.trimIndent()
        getConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(query)
            }
        }
    }
}