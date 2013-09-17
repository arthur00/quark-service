using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuarkService
{
    /// <summary>
    /// Main class for starting Quark Service. Quark Service provides two main functionalities:
    /// 1. Provides actors with servers (parents) to connect to once they start, based on their name/signature
    /// 2. Coordinates the quark crossing mechanism so it is completely consistent. The crossing is performed in the following way:
    /// 
    /// A. Actor requests crossing permission from Sync Service
    /// B. Sync Service denies (if a crossing for that object is already underway) or authorizes
    /// C. Upon authorization, the Sync Quark Crossing Msg is sent to all other actors and the object is uploaded to the root, if required.
    /// Upon receiving the quark crossing, actors should ACK the sync service. Also, every actor that receives a crossing message will stop 
    /// syncing new updates until a crossing finished is received. 
    /// D. When the sync service realizes all actors have been accounted for, it informs the Root actor to push a quark crossing finished,
    /// which release updates to be synced again and new crossings to happen. 
    /// </summary>
    class QuarkService
    {
        static void Main(string[] args)
        {
            QuarkServiceListener handler = new QuarkServiceListener();
        }
    }
}
