using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using Dapper;
using Npgsql;

public class PaymentBatchInserter
{
    private ConcurrentQueue<PaymentInsertParameters> Buffer { get; } = new();
    private int BatchSize { get; } = 100;

    private IDbConnection DbConnection { get; }
    private BusyInstanceTracker Tracker { get; }

    public PaymentBatchInserter(IDbConnection dbConnection, [FromKeyedServices("tracker:postgres")] BusyInstanceTracker tracker)
    {
        DbConnection = dbConnection ?? throw new ArgumentNullException(nameof(dbConnection));
        Tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
    }

    public async Task<int> AddAsync(PaymentInsertParameters payment)
    {
        await Tracker.IncrementAsync().ConfigureAwait(false);
        Buffer.Enqueue(payment);

        if (Buffer.Count >= BatchSize)
        {
            Console.WriteLine($"[Batch] Buffer reached batch size ({BatchSize}). Flushing batch...");
            return await FlushBatchAsync().ConfigureAwait(false);
        }
        return 0;
    }



    public async Task<int> FlushBatchAsync()
    {
        if (Buffer.IsEmpty)
            return 0;

        int totalInserted = 0;

        if (DbConnection is not NpgsqlConnection npgsqlConn)
            throw new InvalidOperationException("DbConnection must be an NpgsqlConnection.");

        string connectionString = npgsqlConn.ConnectionString;

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync().ConfigureAwait(false);

        var totalStopwatch = Stopwatch.StartNew();

        while (!Buffer.IsEmpty)
        {
            var batch = new List<PaymentInsertParameters>(BatchSize);
            while (batch.Count < BatchSize && Buffer.TryDequeue(out var item))
                batch.Add(item);

            if (batch.Count == 0)
                break;

            var batchStopwatch = Stopwatch.StartNew();

            var sqlBuilder = new System.Text.StringBuilder();
            var parameters = new DynamicParameters();

            sqlBuilder.AppendLine("INSERT INTO payments (correlation_id, processor, amount, requested_at) VALUES");

            for (int i = 0; i < batch.Count; i++)
            {
                var p = batch[i];
                var suffix = i.ToString();

                sqlBuilder.AppendLine($"(@CorrelationId{suffix}, @Processor{suffix}, @Amount{suffix}, @RequestedAt{suffix}){(i < batch.Count - 1 ? "," : ";")}");

                parameters.Add($"CorrelationId{suffix}", p.CorrelationId);
                parameters.Add($"Processor{suffix}", p.Processor);
                parameters.Add($"Amount{suffix}", p.Amount);
                parameters.Add($"RequestedAt{suffix}", p.RequestedAt);
            }

            string finalSql = sqlBuilder.ToString();

            await conn.ExecuteAsync(finalSql, parameters).ConfigureAwait(false);

            batchStopwatch.Stop();

            //Console.WriteLine($"Inserted batch of {batch.Count} records in {batchStopwatch.ElapsedMilliseconds} ms");

            totalInserted += batch.Count;

            await Tracker.DecrementAsync(batch.Count).ConfigureAwait(false);
        }

        totalStopwatch.Stop();

        //Console.WriteLine($"Inserted total {totalInserted} records in {totalStopwatch.ElapsedMilliseconds} ms");

        return totalInserted;
    }

}
