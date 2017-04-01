using Apache.Ignite.Core.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace testIgnite
{
    
    class TestObject: IBinarizable
    {

        private Int32 id;
        private String name;

        public string NAME
        {
            get
            {
                return this.name;
            }
            set
            {
                this.name = value;
            }
        }


        public Int32 ID
        {
            get
            {
                return this.id;
            }
            set
            {
                this.id = value;
            }
        }


        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("ID", ID);
            writer.WriteString("NAME", NAME);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            ID = reader.ReadInt("ID");
            NAME = reader.ReadString("NAME");   
        }

    }
}
