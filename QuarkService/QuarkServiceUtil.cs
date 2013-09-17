using System;
using System.Net;
using log4net;
using System.Reflection;
using System.IO;
using System.Xml.Serialization;
using System.Collections.Generic;
using OpenMetaverse;

namespace QuarkService
{
    /// <summary>
    /// Helper class for quark resolution
    /// </summary>
    public class SyncQuark
    {
        public static int SizeX; // Must be set by QuarkManager. Default will be 256.
        public static int SizeY;
        public bool ValidQuark = false; //"false" indicates having not been assigned a location yet
        private int m_quarkLocX;
        private int m_quarkLocY;
        private string m_quarkName = String.Empty;

        public int QuarkLocX
        {
            get { return m_quarkLocX; }
            //set {m_quarkLocX = value;}
        }
        public int QuarkLocY
        {
            get { return m_quarkLocY; }
            //set {m_quarkLocY = value;}
        }
        public string QuarkName
        {
            get { return m_quarkName; }
        }

        public SyncQuark(string quarkName)
        {
            m_quarkName = quarkName;
            DecodeSyncQuarkLoc();
            ComputeMinMax();
        }

        // Create a SyncQuark given a position in the region domain
        public SyncQuark(Vector3 pos)
        {
            int locX, locY;
            GetQuarkLocByPosition(pos, out locX, out locY);
            m_quarkLocX = locX;
            m_quarkLocY = locY;
            ValidQuark = true;
            m_quarkName = SyncQuarkLocToName(locX, locY);
            ComputeMinMax();
        }

        private void ComputeMinMax()
        {
            if (ValidQuark)
            {
                m_minX = m_quarkLocX * SizeX;
                m_minY = m_quarkLocY * SizeY;

                m_maxX = m_minX + SizeX;
                m_maxY = m_minY + SizeY;
            }
        }

        public override bool Equals(Object other)
        {
            if (other != null && other is SyncQuark)
            {
                SyncQuark sq = other as SyncQuark;
                return (this.m_quarkLocX == sq.m_quarkLocX) && (this.m_quarkLocY == sq.m_quarkLocY);
            }
            return false;
        }

        public override string ToString()
        {
            return m_quarkName;
        }

        public override int GetHashCode()
        {
            return m_quarkName.GetHashCode();
        }

        #region Util functions
        public static void SyncQuarkNameToLoc(string quarkName, out int quarkLocX, out int quarkLocY)
        {
            quarkLocX = -1;
            quarkLocY = -1;

            string[] stringItems = quarkName.Split(',');
            if (stringItems.Length != 2)
                return;

            quarkLocX = Convert.ToInt32(stringItems[0]);
            quarkLocY = Convert.ToInt32(stringItems[1]);
        }

        public static string SyncQuarkLocToName(int qLocX, int qlocY)
        {
            string quarkName = qLocX + "," + qlocY;
            return quarkName;
        }

        /// <summary>
        /// Given a global coordinate, return the location of the quark the 
        /// position resides in. Assumption: 
        /// (1) All quarks fit within one OpenSim
        /// region. Hence, Constants.RegionSize is the max value for both x,y
        /// coordinates of a position.
        /// (2) The base locX,Y values are (0,0).
        /// </summary>
        /// <param name="pos"></param>
        /// <param name="quarkLocX"></param>
        /// <param name="quarkLocY"></param>
        public static void GetQuarkLocByPosition(Vector3 pos, out int quarkLocX, out int quarkLocY)
        {
            quarkLocX = (int)(pos.X / SizeX);
            quarkLocY = (int)(pos.Y / SizeY);
        }

        public static string GetQuarkNameByPosition(Vector3 pos)
        {
            if (pos == null)
                pos = new Vector3(0, 0, 0);
            int qLocX, qLocY;
            GetQuarkLocByPosition(pos, out qLocX, out qLocY);
            string quarkName = SyncQuarkLocToName(qLocX, qLocY);
            return quarkName;
        }

        #endregion

        #region SyncQuark Members and functions

        private int m_minX, m_minY, m_maxX, m_maxY;

        public bool IsPositionInQuark(Vector3 pos)
        {
            if (pos.X >= m_minX && pos.X < m_maxX && pos.Y >= m_minY && pos.Y < m_maxY)
            {
                return true;
            }
            return false;
        }

        private void DecodeSyncQuarkLoc()
        {
            int qLocX, qLocY;
            SyncQuarkNameToLoc(m_quarkName, out qLocX, out qLocY);

            if (qLocX == -1)
            {
                ValidQuark = false;
                return;
            }

            m_quarkLocX = qLocX;
            m_quarkLocY = qLocY;
            ValidQuark = true;
        }

        static public HashSet<string> DecodeSyncQuarks(string quarksInput)
        {
            if (quarksInput.Equals(String.Empty))
                return new HashSet<string>();

            //each input string should be in the format of "xl[-xr] or x, yl[-yr] or y/.../...", 
            //where "xl[-xr],yl[-yr]" specifies a range of quarks (a quark block, where 
            //"xl,yl" is the x,y indices for the lower left corner quark, and "xr,yr" is 
            //the x,y indices for the upper right corner quark.
            //x and y indices of a quark is calculated by floor(x/quark_size), and 
            //floor(y/quark_size), where x,y is one position that is within the quark.
            string interQuarkDelimStr = "/";
            char[] interQuarkDelimeter = interQuarkDelimStr.ToCharArray();
            string[] quarkSet = quarksInput.Split(interQuarkDelimeter);

            string intraQuarkDelimStr = ",";
            char[] intraQuarkDelimeter = intraQuarkDelimStr.ToCharArray();
            string xyDelimStr = "-";
            char[] xyDelimeter = xyDelimStr.ToCharArray();
            HashSet<string> quarksOutput = new HashSet<string>();

            foreach (string quarkString in quarkSet)
            {
                string[] quarkXY = quarkString.Split(intraQuarkDelimeter);
                if (quarkXY.Length < 2)
                {
                    //m_log.WarnFormat("DecodeSyncQuarks: Invalid quark configuration: {0}", quarkString);
                    continue;
                }
                string qX = quarkXY[0];
                string qY = quarkXY[1];

                //Are X,Y specified as "xl[-xr],yl[-yr]", "x,y", "xl[-xr],y", or "x,yl[-yr]"?
                string[] xRange = qX.Split(xyDelimeter);
                int xLow = 0, xHigh = -1;
                if (xRange.Length == 2)
                {
                    int.TryParse(xRange[0], out xLow);
                    int.TryParse(xRange[1], out xHigh);
                }
                else if (xRange.Length == 1)
                {
                    int.TryParse(xRange[0], out xLow);
                    xHigh = xLow;
                }
                else
                {
                    //m_log.WarnFormat("DecodeSyncQuarks: Invalid quark configuration: {0}", quarkString);
                }

                string[] yRange = qY.Split(xyDelimeter);
                int yLow = 0, yHigh = -1;
                if (yRange.Length == 2)
                {
                    int.TryParse(yRange[0], out yLow);
                    int.TryParse(yRange[1], out yHigh);
                }
                else if (yRange.Length == 1)
                {
                    int.TryParse(yRange[0], out yLow);
                    yHigh = yLow;
                }
                else
                {
                    //m_log.WarnFormat("DecodeSyncQuarks: Invalid quark configuration: {0}", quarkString);
                }

                for (int x = xLow; x <= xHigh; x++)
                {
                    for (int y = yLow; y <= yHigh; y++)
                    {
                        string quarkName = String.Format("{0},{1}", x, y);

                        quarksOutput.Add(quarkName);
                    }
                }
            }
            return quarksOutput;
        }
        #endregion
    }

    /// <summary>
    /// QuarkPublisher
    /// Description: Stores all SyncConnectors subscribed actively and passively to a quark. Only quarks that belong to this sync process.
    /// </summary>
    public class QuarkPublisher
    {
        //private static ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private SyncQuark m_quark;
        private string m_quarkName;
        private string m_root = "";
        public string RootActorID { get { return m_root; } }
        private string LogHeader = "[QUARKPUBLISHER]";

        private HashSet<string> m_passiveQuarkSubscribers = new HashSet<string>();
        public HashSet<string> PassiveSubscribers { get { return m_passiveQuarkSubscribers; } }

        private HashSet<string> m_activeQuarkSubscribers = new HashSet<string>();
        public HashSet<string> ActiveSubscribers { get { return m_activeQuarkSubscribers; } }

        public QuarkPublisher(SyncQuark quark)
        {
            m_quarkName = quark.QuarkName;
            m_quark = quark;
        }

        public void SetRootActor(string syncID)
        {
            if (m_root.Length > 0)
            {
                // There is already a root defined for this quark! assuming it was disconnected and replacing.
                RemoveSubscriber(m_root);
            }
            m_root = syncID;
        }

        /// <summary>
        /// Adds a new connector to PassiveSubscribers
        /// </summary>
        /// <param name="connector"></param>
        public void AddPassiveSubscriber(string syncID)
        {
            m_passiveQuarkSubscribers.Add(syncID);
        }

        /// <summary>
        /// Adds a new connector to ActiveSubscribers
        /// </summary>
        /// <param name="connector"></param>
        public void AddActiveSubscriber(string syncID)
        {
            m_activeQuarkSubscribers.Add(syncID);
        }


        /// <summary>
        /// Iterates over every quark subscription and deletes the connector from it. 
        /// TODO: This seems slow, any way to make it better?
        /// </summary>
        /// <param name="connector"></param>
        public void RemoveSubscriber(string syncID)
        {
            if (m_activeQuarkSubscribers.Contains(syncID))
            {
                m_activeQuarkSubscribers.Remove(syncID);
                if (syncID == m_root)
                {
                    // Root is gone! Usually not a good sign
                    m_root = "";
                    //m_log.ErrorFormat("{0}: Root actor has been removed", LogHeader);
                }
            }
            if (m_passiveQuarkSubscribers.Contains(syncID))
            {
                m_passiveQuarkSubscribers.Remove(syncID);
            }
        }

        /// <summary>
        /// Returns all subscribers with QuarkName (both active and passive)
        /// </summary>
        /// <returns>Union of the active and passive syncconnectors subscribed to this quark</returns>
        public HashSet<string> GetAllQuarkSubscribers()
        {
            HashSet<string> subscribers = new HashSet<string>(m_activeQuarkSubscribers);
            subscribers.UnionWith(m_passiveQuarkSubscribers);
            return subscribers;
        }
    }

    public class RootInfo : RelayActor
    {
        public string quarkAddress;

        public RootInfo()
        {
        }

        public RootInfo(string syncAdd, string quarkAdd)
        {
            syncAddress = syncAdd;
            quarkAddress = quarkAdd;
        }
    }
    

    public class RelayActor : Actor
    {
        public string syncAddress;
    }

    public class Actor
    {
        public HashSet<RootInfo> Roots = new HashSet<RootInfo>();
        public XMLQuarkSubscription XmlSubscription = null;
        public HashSet<string> ActiveQuarks = null;
        public HashSet<string> PassiveQuarks = null;
    }


    public class XMLQuarkSubscription
    {
        public string activeQuarks = "";
        public string passiveQuarks = "";
        public string actorID = "";
        public string syncListenerAddress = "";
        public string rootAddress = "";
    }

    public class CurrentCrossings
    {
        public CrossingRequest cross;
        public HashSet<string> actors = new HashSet<string>();
        public RootInfo rootHandler;
    }

    [Serializable, XmlRoot("CrossingRequest")]
    public class CrossingRequest
    {
        public long timestamp = 0;
        public UUID uuid = UUID.Zero;
        public string curQuark = "";
        public string prevQuark = "";
        public string url = "";
        public string actorID = "";
    }

    public class CrossingFinished
    {
        public string ackActorID = "";
        public CrossingRequest cross = null;
    }
}
