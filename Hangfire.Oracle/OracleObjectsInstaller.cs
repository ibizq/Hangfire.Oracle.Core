using Dapper;
using Hangfire.Logging;
using System;
using System.Data;
using System.IO;
using System.Reflection;

namespace Hangfire.Oracle.Core
{
    public static class OracleObjectsInstaller
    {
        private static readonly ILog Log = LogProvider.GetLogger(typeof(OracleStorage));

        public static void Install(IDbConnection connection, string schemaName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            if (TablesExists(connection, schemaName))
            {
                Log.Info("DB tables already exist. Exit install");
                return;
            }

            Log.Info("Start installing Hangfire SQL objects...");

            try
            {
                InstallationFromFile(connection);
            }
            catch (Exception ex)
            {
                InstallationFromCode(connection);
                Log.ErrorException(ex.Message, ex);
            }

            Log.Info("Hangfire SQL objects installed.");
        }

        private static void InstallationFromFile(IDbConnection connection)
        {
            var script = GetStringResource("Hangfire.Oracle.Core.Install.sql");
            var scripts = script.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);

            using (var scope = BeginTransactionScope())
            {
                try
                {
                    foreach (var sql in scripts)
                    {
                        connection.Execute(sql);
                    }
                }
                catch (Exception ex)
                {
                    Log.ErrorException("Hangfire SQL error.", ex);
                    throw;
                }

                // Complete the transaction scope if all commands succeed
                scope.Complete();
            }
        }

        private static void InstallationFromCode(IDbConnection connection)
        {
            using (var scope = BeginTransactionScope())
            {
                try
                {
                    connection.Execute(InstallationCommands.Command01);
                    connection.Execute(InstallationCommands.Command02);
                    connection.Execute(InstallationCommands.Command03);
                    connection.Execute(InstallationCommands.Command04);
                    connection.Execute(InstallationCommands.Command05);
                    connection.Execute(InstallationCommands.Command06);
                    connection.Execute(InstallationCommands.Command07);
                    connection.Execute(InstallationCommands.Command08);
                    connection.Execute(InstallationCommands.Command09);
                    connection.Execute(InstallationCommands.Command10);
                    connection.Execute(InstallationCommands.Command11);
                    connection.Execute(InstallationCommands.Command12);
                    connection.Execute(InstallationCommands.Command13);
                    connection.Execute(InstallationCommands.Command14);
                    connection.Execute(InstallationCommands.Command15);
                    connection.Execute(InstallationCommands.Command16);
                    connection.Execute(InstallationCommands.Command17);
                    connection.Execute(InstallationCommands.Command18);
                    connection.Execute(InstallationCommands.Command19);
                    connection.Execute(InstallationCommands.Command20);
                    connection.Execute(InstallationCommands.Command21);
                    connection.Execute(InstallationCommands.Command22);
                    connection.Execute(InstallationCommands.Command23);
                    connection.Execute(InstallationCommands.Command24);
                    connection.Execute(InstallationCommands.Command25);
                    connection.Execute(InstallationCommands.Command26);

                    // Complete the transaction scope if all commands succeed
                    scope.Complete();
                }
                catch (Exception ex)
                {
                    Log.ErrorException(ex.Message, ex);
                    throw;
                }
            }
        }

        private static bool TablesExists(IDbConnection connection, string schemaName)
        {
            return connection.ExecuteScalar<string>($@"
   SELECT TABLE_NAME
     FROM all_tables
    WHERE OWNER = '{schemaName}' AND TABLE_NAME LIKE 'HF_%'
 ORDER BY TABLE_NAME
") != null;
        }

        private static string GetStringResource(string resourceName)
        {
#if NET45
            var assembly = typeof(OracleObjectsInstaller).Assembly;
#else
            var assembly = typeof(OracleObjectsInstaller).GetTypeInfo().Assembly;
#endif

            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException($"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        private static System.Transactions.TransactionScope BeginTransactionScope()
        {
            return new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Required, new System.Transactions.TransactionOptions
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromMinutes(5)
            });
        }
    }
}