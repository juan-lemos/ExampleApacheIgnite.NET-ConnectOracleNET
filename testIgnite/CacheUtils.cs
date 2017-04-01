using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Cache.Store;
using Apache.Ignite.Core.Common;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Net.NetworkInformation;
using System.Xml;
using System.Xml.Linq;

namespace testIgnite
{
    
    public class OracleStore : CacheStoreAdapter
    {
        public readonly static string info = "Data Source=ip/orcl12c;User Id=user;Password=pass;Pooling=true;Statement Cache Size=20;Enlist=false;";
        public override object Load(object key)
        {
            using (var con = new OracleConnection
            {
                ConnectionString = info
            })
            {
                con.Open();

                var cmd = con.CreateCommand();
                cmd.CommandText = "SELECT * FROM TESTOBJECT WHERE ID=:id";
                cmd.BindByName = true;
                cmd.Parameters.Add("id", OracleDbType.Int32, (Int32)key, ParameterDirection.Input);

                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    // Read data, return as object
                    TestObject obj = new TestObject();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        String columnName = reader.GetName(i);
                        if (columnName.Equals("ID"))
                        {
                            obj.ID = reader.GetInt32(i);
                        }
                        else if (columnName.Equals("NAME"))
                        {
                            obj.NAME = reader.GetString(i);
                        }
                    }
                    return obj;
                }
            }
        }
        


        public override void Write(object key, object val)
        {
            OracleConnection conn = new OracleConnection(info);
            OracleTransaction txn = conn.BeginTransaction();
            OracleCommand cmd = new OracleCommand();
            // Set the command text on an OracleCommand object
            cmd.CommandText = @"BEGIN
              INSERT INTO TESTOBJECT(ID, NAME) VALUES(:key,:val );
                        EXCEPTION
                          WHEN DUP_VAL_ON_INDEX THEN
                UPDATE TESTOBJECT
                SET name = :val
                WHERE id = :key;
                        END; ";
            cmd.Connection = conn;
            cmd.BindByName = true;  ///////because i will name the paramaters
            cmd.Parameters.Add("key", OracleDbType.Int32, (Int32)key, ParameterDirection.Input);
            cmd.Parameters.Add("val", OracleDbType.Varchar2, ((TestObject)val).NAME, ParameterDirection.Input);

            // Execute the command
            cmd.ExecuteNonQuery();
            txn.Commit();

            txn.Dispose();
            foreach (OracleParameter p in cmd.Parameters)
            {
                p.Dispose();
            }
            cmd.Dispose();
            conn.Close();
            conn.Dispose();

        }

        public override void Delete(object key)
        {
            OracleConnection conn = new OracleConnection(info);
            OracleTransaction txn = conn.BeginTransaction();
            OracleCommand cmd = new OracleCommand();
            // Set the command text on an OracleCommand object
            cmd.CommandText = @"delete from TESTOBJECT Where ID=:key ";
            cmd.Connection = conn;
            cmd.BindByName = true;  ///////because i will name the paramaters
            cmd.Parameters.Add("key", OracleDbType.Int32, (Int32)key, ParameterDirection.Input);
            // Execute the command
            cmd.ExecuteNonQuery();
            txn.Commit();

            txn.Dispose();
            foreach (OracleParameter p in cmd.Parameters)
            {
                p.Dispose();
            }
            cmd.Dispose();
            conn.Close();
            conn.Dispose();
        }
    }

    [Serializable]
    public class OracleStoreFactory : IFactory<OracleStore>
    {
        public OracleStore CreateInstance()
        {
            return new OracleStore();
        }
    }

    class CacheUtils
    {

        public static void startCache()
        {
            IgniteConfiguration cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof (TestObject))
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cacheCfg = new CacheConfiguration
                {
                    ReadThrough = true,
                    WriteThrough = true,
                    KeepBinaryInStore = false, 
                    CacheStoreFactory = new OracleStoreFactory()
                };


                var cache = ignite.CreateCache<int, TestObject>(cacheCfg);
                Console.WriteLine(cache.Get(1).ID+"  "+ cache.Get(1).NAME);  // OracleStore.Load is called.
            }
        }
    }
}
