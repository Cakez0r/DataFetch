using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace DataFetch
{
    /// <summary>
    /// Fetches data from a database (I.E. Postgres, MySQL, MSSQL)
    /// </summary>
    /// <typeparam name="DbConnectionType">The database connection type to use</typeparam>
    public static class DataFetcher<DbConnectionType> where DbConnectionType : IDbConnection, new()
    {
        /// <summary>
        /// Reflection flags to get public private instance properties
        /// </summary>
        private const BindingFlags BINDING_FLAGS = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        /// <summary>
        /// Execute a stored procedure on the database
        /// </summary>
        /// <typeparam name="POCOType">The POCO type that matches the stored procedure result set</typeparam>
        /// <param name="storedProcedureName">The name of the stored procedure to execute</param>
        /// <param name="connectionString">The connection string to use for the database</param>
        /// <param name="parameters">The parameters to bind to the stored procedure</param>
        /// <returns>An enumerable result set of POCOs returned by the database</returns>
        public static IEnumerable<POCOType> StoredProcedure<POCOType>(string storedProcedureName, string connectionString, object parameters) where POCOType : new()
        {
            return Fetch<POCOType>(storedProcedureName, CommandType.StoredProcedure, connectionString, parameters);
        }

        /// <summary>
        /// Execute a command on the database
        /// </summary>
        /// <typeparam name="POCOType">The POCO type that matches the command result set</typeparam>
        /// <param name="commandText">The command to execute</param>
        /// <param name="connectionString">The connection string to use for the database</param>
        /// <returns>An enumerable result set of POCOs returned by the database</returns>
        public static IEnumerable<POCOType> Command<POCOType>(string commandText, string connectionString) where POCOType : new()
        {
            return Fetch<POCOType>(commandText, CommandType.Text, connectionString, null);
        }

        private static IEnumerable<POCOType> Fetch<POCOType>(string commandText, CommandType commandType, string connectionString, object parameters) where POCOType : new()
        {
            //Create a connection
            using (DbConnectionType connection = new DbConnectionType())
            {
                //Connect
                connection.ConnectionString = connectionString;
                connection.Open();

                //Create the command
                using (IDbCommand command = connection.CreateCommand())
                {
                    //Setup the command
                    command.CommandType = commandType;
                    command.CommandText = commandText;

                    //Bind any parameters if necessary
                    if (parameters != null)
                    {
                        //Reflect any properties
                        PropertyInfo[] parameterFields = parameters.GetType().GetProperties(BINDING_FLAGS);

                        foreach (PropertyInfo f in parameterFields)
                        {
                            //Create a db parameter for each property on the parameters object
                            IDbDataParameter p = command.CreateParameter();

                            p.ParameterName = f.Name;
                            p.Value = f.GetValue(parameters, null);

                            //Bind it to the command
                            command.Parameters.Add(p);
                        }
                    }

                    //Run the command
                    using (IDataReader reader = command.ExecuteReader())
                    {
                        //Prepare some info for processing results

                        //Get the names of all properties on the POCO
                        PropertyInfo[] resultFields = typeof(POCOType).GetProperties(BINDING_FLAGS);

                        //Keep a mapping of the ordinals for result field index to column ordinal
                        int[] ordinals = new int[resultFields.Length];
                        for (int i = 0; i < resultFields.Length; i++)
                        {
                            ordinals[i] = reader.GetOrdinal(resultFields[i].Name);
                        }

                        //Result buffer
                        object[] values = new object[resultFields.Length];

                        //Start pulling results!
                        while (reader.Read())
                        {
                            //New up a POCO
                            POCOType result = new POCOType();

                            //Get the result values
                            reader.GetValues(values);

                            //Start filling up the POCO with result values
                            for (int i = 0; i < resultFields.Length; i++)
                            {
                                object value = values[ordinals[i]];

                                resultFields[i].SetValue(result, value is DBNull ? null : value, null);
                            }

                            //Yield a POCO representing a row in the resultset
                            yield return result;
                        }

                        reader.Close();
                    }
                }
            }
        }
    }
}
