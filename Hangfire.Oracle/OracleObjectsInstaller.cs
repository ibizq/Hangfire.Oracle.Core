using Dapper;
using Hangfire.Logging;
using System;
using System.Data;

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
                catch (Exception err)
                {
                    Log.Error(err.Message);
                    throw;
                }
            }

            Log.Info("Hangfire SQL objects installed.");
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