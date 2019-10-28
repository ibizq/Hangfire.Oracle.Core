﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using Dapper;

using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Oracle.Core.JobQueue;
using Hangfire.Oracle.Core.Monitoring;
using Hangfire.Server;
using Hangfire.Storage;

using Oracle.ManagedDataAccess.Client;

namespace Hangfire.Oracle.Core
{
    public class OracleStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleStorage));

        private string _string;
        private readonly string _connectionString;
        private readonly Func<IDbConnection> _connectionFactory;
        private readonly OracleStorageOptions _options;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public OracleStorage(string connectionString)
            : this(connectionString, new OracleStorageOptions())
        {
        }

        public OracleStorage(string connectionString, OracleStorageOptions options)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            _options = options ?? throw new ArgumentNullException(nameof(options));

            if (IsConnectionString(connectionString))
            {
                _connectionString = connectionString;
            }
            else
            {
                throw new ArgumentException($"Could not find connection string with name '{connectionString}' in application config file");
            }

            if (options.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    OracleObjectsInstaller.Install(connection, options.SchemaName);
                }
            }

            InitializeQueueProviders();
        }

        public OracleStorage(Func<IDbConnection> connectionFactory, OracleStorageOptions options)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _options = options;

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            QueueProviders = new PersistentJobQueueProviderCollection(new OracleJobQueueProvider(this, _options));
        }

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
        }

        public override string ToString()
        {
            if (!string.IsNullOrWhiteSpace(_string))
            {
                return _string;
            }

            var connectionString = _connectionString;

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                using (var connection = CreateAndOpenConnection())
                {
                    connectionString = connection.ConnectionString;
                }
            }

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                _string = "Hangfire.Oracle.Core";
                return _string;
            }

            try
            {
                var parts = connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x.Length > 1 ? x[1].Trim() : "" })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                _string = $"Hangfire.Oracle.Core: {builder}";
                return _string;
            }
            catch (Exception ex)
            {
                Logger.ErrorException(ex.Message, ex);
                _string = "<Connection string can not be parsed>";
                return _string;
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new OracleMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new OracleStorageConnection(this);
        }

        private static bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        internal void UseTransaction([InstantHandle] Action<IDbConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>([InstantHandle] Func<IDbConnection, T> func, IsolationLevel? isolationLevel)
        {
            return UseConnection(connection =>
            {
                using (var transaction = connection.BeginTransaction(isolationLevel ?? _options.TransactionIsolationLevel ?? IsolationLevel.ReadCommitted))
                {
                    var result = func(connection);
                    transaction.Commit();

                    return result;
                }
            });
        }

        internal void UseConnection([InstantHandle] Action<IDbConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<IDbConnection, T> func)
        {
            IDbConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal IDbConnection CreateAndOpenConnection()
        {
            var connection = _connectionFactory != null ? _connectionFactory() : new OracleConnection(_connectionString);

            if (connection.State == ConnectionState.Closed)
            {
                var retries = 3;

                while ((retries >= 0) && (connection.State != ConnectionState.Open))
                {
                    try
                    {
                        connection.Open();
                        retries--;
                    }
                    catch (Exception ex)
                    {
                        ReleaseConnection(connection);

                        connection = new OracleConnection(_connectionString);
                        connection.Open();

                        Logger.ErrorException(ex.Message, ex);
                    }
                }

                if (!string.IsNullOrWhiteSpace(_options.SchemaName))
                {
                    connection.Execute($"ALTER SESSION SET CURRENT_SCHEMA={_options.SchemaName}");
                }
            }

            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection?.State != ConnectionState.Closed)
            {
                connection?.Close();
            }

            connection?.Dispose();
        }

        public void Dispose()
        {

        }
    }
}
