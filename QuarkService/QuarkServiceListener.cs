using System;
using System.Net;
using log4net;
using System.Reflection;
using System.IO;
using log4net.Config;
using System.Xml.Serialization;
using System.Collections.Generic;
using OpenMetaverse.StructuredData;
using OpenMetaverse;
using System.Threading;

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
    public class QuarkServiceListener
    {
        private System.Threading.ReaderWriterLockSlim m_crossLock = new System.Threading.ReaderWriterLockSlim();
        private ILog m_log;
        private string LogHeader = "[QUARKSERVICE]";
        
        
        
        private bool m_running = false;
        private HttpListener m_listener;
        #region Indexing Dictionaries

        private Dictionary<string, Actor> m_actor = new Dictionary<string, Actor>();
        private HashSet<RootInfo> m_rootActors = new HashSet<RootInfo>();

        private Dictionary<UUID, CurrentCrossings> m_crossings = new Dictionary<UUID, CurrentCrossings>();
        private Dictionary<UUID, CurrentCrossings> m_futureCrossings = new Dictionary<UUID, CurrentCrossings>();

        // Saves which actors are subscribed actively/passively per quark name
        // string: quark name
        private Dictionary<string, QuarkPublisher> m_quarkSubscriptions = new Dictionary<string, QuarkPublisher>();
        public Dictionary<string, QuarkPublisher> QuarkSubscriptions
        {
            get { return m_quarkSubscriptions; }
        }

        #endregion //Indexing Dictionaries

        public QuarkServiceListener()
        {
            m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
            BasicConfigurator.Configure();
            Start();
        }

        public void Stop()
        {
            if (m_running)
                m_listener.Stop();
        }

        public void Start()
        {

            if (!HttpListener.IsSupported)
            {
                Console.WriteLine("Windows XP SP2 or Server 2003 is required to use the QuarkHttpListener class.");
                return;
            }

            IPHostEntry host;
            string localIP = "";
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIP = ip.ToString();
                }
            }

            // Create a listener.
            m_listener = new HttpListener();

            // DELETEME
                localIP = "localhost";
                // For now, using star topology. Every node conects to a parent node. 
            // </DELETEME>
            m_listener.Prefixes.Add("http://" + localIP + ":8080/quark/");
            m_listener.Start();
            m_running = true;
            m_log.Info("[QUARKSERVICE]: Listening on " + localIP + ":8080");
            while (m_running)
            {
                // Note: The GetContext method blocks while waiting for a request. 
                HttpListenerContext context = m_listener.GetContext();
                m_log.InfoFormat("{0}: Received request. Time: {1}", LogHeader, DateTime.Now.Ticks);
                HttpListenerRequest request = context.Request;
                try
                {
                    if (request.RawUrl.Contains("/quark/subscribe"))
                        HandleSubscribe(context, request);
                    else if (request.RawUrl.Contains("/quark/cross"))
                        HandleCrossing(context, request);
                    else if (request.RawUrl.Contains("/quark/hello"))
                        HandleHelloWorld(context, request);
                    else if (request.RawUrl.Contains("/quark/cancel"))
                        HandleCancelCrossing(context, request);
                    else if (request.RawUrl.Contains("/quark/ack"))
                        HandleAckCrossing(context, request);
                }
                catch (Exception e)
                {
                    m_log.Error("[QUARKSERVICE]: HTTP request could not be handled.", e);
                }
            }
        }

        // Test method for HttpRequests
        private void HandleHelloWorld(HttpListenerContext context, HttpListenerRequest request)
        {
            m_log.InfoFormat("{0}: Hello World! Your request was {1}",LogHeader,request.RawUrl);
            HttpListenerResponse response = context.Response;
            response.StatusCode = (int)HttpStatusCode.OK;
            byte[] rootData = System.Text.Encoding.ASCII.GetBytes("Hello World");
            int rootDataLength = rootData.Length;
            response.OutputStream.Write(rootData, 0, rootDataLength);
            response.Close();
        }

        /// <summary>
        /// Used for actors ACKing received crossed messages. Once all expected actors have ACKEd the crossing message,
        /// the Sync Service informs the root actor to push a crossing finished.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="request"></param>
        private void HandleAckCrossing(HttpListenerContext context, HttpListenerRequest request)
        {
            m_log.InfoFormat("{0}: HandleAckCrossing",LogHeader);
            StreamReader actorReader = new StreamReader(request.InputStream);
            XmlSerializer deserializer = new XmlSerializer(typeof(CrossingFinished));
            CrossingFinished cf = (CrossingFinished)deserializer.Deserialize(actorReader);
            string ackActorID = cf.ackActorID;
            CrossingRequest cross = cf.cross;
            RootInfo root = null;

            bool allAcksReceived = false;

            try
            {
                HttpStatusCode actorStatus;
                m_crossings[cross.uuid].actors.Remove(ackActorID);
                m_log.InfoFormat("{0}: Ack received from {1}, {2} acks remaining.", LogHeader, ackActorID, m_crossings[cross.uuid].actors.Count);
                if (m_crossings[cross.uuid].actors.Count == 0)
                {
                    root = m_crossings[cross.uuid].rootHandler;
                    actorStatus = CancelCrossing(cross);
                    allAcksReceived = true;
                }
                else
                    actorStatus = HttpStatusCode.OK;    

                HttpListenerResponse actorResponse = context.Response;
                actorResponse.StatusCode = (int)actorStatus;
                actorResponse.Close();

                if (allAcksReceived)
                {
                    if (actorStatus == HttpStatusCode.OK)
                    {
                        m_log.InfoFormat("{0}: Informing root that crossing is finished", LogHeader);
                        // Now tell root to tell every actor that the crossing is finnished!
                        // Root's request variables
                        HttpWebRequest rootRequest = (HttpWebRequest)WebRequest.Create("http://" + root.quarkAddress + "/finished/");
                        rootRequest.Credentials = CredentialCache.DefaultCredentials;
                        rootRequest.Method = "POST";
                        rootRequest.ContentType = "text/json";
                        Stream rootOutput = rootRequest.GetRequestStream();

                        OSDMap DataMap = new OSDMap();
                        DataMap["uuid"] = OSD.FromUUID(cross.uuid);
                        DataMap["ts"] = OSD.FromLong(cross.timestamp);

                        string encodedMap = OSDParser.SerializeJsonString(DataMap, true);
                        byte[] rootData = System.Text.Encoding.ASCII.GetBytes(encodedMap);
                        int rootDataLength = rootData.Length;
                        rootOutput.Write(rootData, 0, rootDataLength);
                        rootOutput.Close();

                        // Check if everything went OK
                        try
                        {
                            HttpWebResponse response = (HttpWebResponse)rootRequest.GetResponse();
                            if (response.StatusCode == HttpStatusCode.OK)
                            {
                                m_log.InfoFormat("{0}: Successfully finished crossing.", LogHeader);
                                return;
                            }
                        }
                        catch (WebException we)
                        {
                            var resp = we.Response as HttpWebResponse;
                            m_log.ErrorFormat("{0}: Sync Crossing finished fail with actorStatus code: {1}", LogHeader, resp.StatusCode);
                        }
                    }
                    else
                    {
                        throw new Exception("Could not find the object in the crossing dictionary");
                    }
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("{0}: Unkown exception while handling ACK from actor. Exception {1}", LogHeader, e);
            }
        }

        /// <summary>
        /// Receives actor subscriptions and informs what parent they should connect to.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="request"></param>
        private void HandleSubscribe(HttpListenerContext context, HttpListenerRequest request)
        {
            StreamReader reader = new StreamReader(request.InputStream);
            XmlSerializer deserializer = new XmlSerializer(typeof(XMLQuarkSubscription));
            XMLQuarkSubscription quarkSub = (XMLQuarkSubscription)deserializer.Deserialize(reader);

            HttpListenerResponse response = context.Response;
            response.StatusCode = (int)HttpStatusCode.OK;
            Stream output = response.OutputStream;
            // Default algorithm is set for star topology.
            if (request.RawUrl.Contains("/quark/subscribe/actor"))
            {
                RegisterActor(quarkSub, false);
                // Construct a response. 
                // Currently sending all roots. Ideally, a policy/algorithm should decide to what 
                List<string> allRoots = new List<string>();
                foreach (RootInfo ri in m_rootActors)
                {
                    allRoots.Add(ri.syncAddress);
                }
                
                System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(allRoots.GetType());

                x.Serialize(output, allRoots);
                
            }
            else if (request.RawUrl.Contains("/quark/subscribe/root"))
            {
                RegisterActor(quarkSub, true);
            }
            // TODO: Should root actors know about other root actors? Currently, we reply nothing to root actors.
            output.Close();
            response.Close();
        }

        /// <summary>
        /// Check if the requested crossing moves the object out of an active quark to non-active quark. 
        /// If not, a simple udpdate property could be used instead, with the added curquark and prevquark info.
        /// </summary>
        /// <param name="cross"></param>
        /// <returns></returns>
        private HttpStatusCode ValidateCrossing(CrossingRequest cross)
        {
            bool crossing = false;
            HashSet<string> subscribedActors = new HashSet<string>();
            subscribedActors.UnionWith(m_quarkSubscriptions[cross.prevQuark].ActiveSubscribers);
            subscribedActors.UnionWith(m_quarkSubscriptions[cross.curQuark].ActiveSubscribers);

            foreach (string strActor in subscribedActors)
            {
                if (!(m_actor[strActor].ActiveQuarks.Contains(cross.prevQuark) && m_actor[strActor].ActiveQuarks.Contains(cross.curQuark)))
                {
                    crossing = true;
                    break;
                }
            }
            if (crossing == false)
                return HttpStatusCode.OK;

            // Crossing is already underway for this object
            if (m_crossings.ContainsKey(cross.uuid))
            {
                // If incoming crossing is older than current, discard, return forbidden.
                // This could happen if two crossings were generated while one is in progress. The crossing with timestamp
                // in between will simply be overwritten
                if (cross.timestamp < m_crossings[cross.uuid].cross.timestamp)
                {
                    return HttpStatusCode.Forbidden;
                }
                else if (cross.timestamp == m_crossings[cross.uuid].cross.timestamp)
                {
                    return HttpStatusCode.Forbidden;
                }
                else
                {
                    // Tell actor there was a conflict. Actor will wait until it receives the first crossing message before trying again.
                    // Note: This could be made more efficient if it's possible to remember the cross requests and only accept the next earliest crossing.
                    return HttpStatusCode.Conflict;
                }
            }
            else
                return HttpStatusCode.Created;
        }


        /// <summary>
        /// Used for cancelling a crossing underway. Should be used before the crossing message starts propagating.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="actorRequest"></param>
        private void HandleCancelCrossing(HttpListenerContext context, HttpListenerRequest actorRequest)
        {
            StreamReader actorReader = new StreamReader(actorRequest.InputStream);
            XmlSerializer deserializer = new XmlSerializer(typeof(CrossingRequest));
            CrossingRequest cross = (CrossingRequest)deserializer.Deserialize(actorReader);
            HttpStatusCode status = CancelCrossing(cross);

            HttpListenerResponse actorResponse = context.Response;
            actorResponse.StatusCode = (int)status;
            actorResponse.Close();
        }

        private HttpStatusCode CancelCrossing(CrossingRequest cross)
        {
            m_crossLock.EnterUpgradeableReadLock();
            try
            {
                if (m_crossings.ContainsKey(cross.uuid))
                {
                    m_crossLock.EnterWriteLock();
                    try
                    {
                        m_crossings.Remove(cross.uuid);
                        return HttpStatusCode.OK;
                    }
                    finally
                    {
                        m_crossLock.ExitWriteLock();
                    }
                }
                else
                    return HttpStatusCode.NotFound;
            }
            finally
            {
                m_crossLock.ExitUpgradeableReadLock();
            }
        }

        /// <summary>
        /// Handles a quark crossing request. Checks to see if the request is possible, and returns true or false based on
        /// the validity of the crossing to the actor who requested. This is performed in 3 steps:
        /// Step 1: Tell root to be ready to receive crossing requests for the object
        /// Root will also do sanity checks (e.g. object was deleted before crossing), so checking actorStatus code
        /// is important.
        /// Step 2: Root provides URL for uploading updated properties and for downloading said object.
        /// Step 3: Tell actor about the URL where it may contact the root directly.
        /// TODO: This can be optimized if we are allowed to remember or calculate the URL. We could respond immediately with the URL
        /// and the actor would keep trying it until root accepts it. For now, we avoid concurrency. 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="actorRequest"></param>
        private void HandleCrossing(HttpListenerContext context, HttpListenerRequest actorRequest)
        {
            StreamReader actorReader = new StreamReader(actorRequest.InputStream);
            XmlSerializer deserializer = new XmlSerializer(typeof(CrossingRequest));
            CrossingRequest cross = (CrossingRequest)deserializer.Deserialize(actorReader);
            QuarkPublisher curQp = null, prevQp = null;
            string url = "";
            HttpStatusCode status;
            m_crossLock.EnterReadLock();
            try
            {
                m_log.InfoFormat("{0}: Handling Crossing. Time: {1}", LogHeader, DateTime.Now.Ticks);
                if (m_quarkSubscriptions.TryGetValue(cross.curQuark, out curQp) && m_quarkSubscriptions.TryGetValue(cross.prevQuark, out prevQp))
                {
                    if (curQp.RootActorID != prevQp.RootActorID)
                    {
                        // TODO: Inter-root communication
                    }
                    else
                    {
                        // Actor's response variables
                        HttpListenerResponse actorResponse = context.Response;
                        Stream actorOutput = actorResponse.OutputStream;

                        // Root's request variables
                        RootInfo root = (RootInfo)m_actor[curQp.RootActorID];
                        HttpWebRequest rootRequest = (HttpWebRequest)WebRequest.Create("http://" + root.quarkAddress + "/cross/");
                        rootRequest.Credentials = CredentialCache.DefaultCredentials;
                        rootRequest.Method = "POST";
                        rootRequest.ContentType = "text/json";
                        Stream rootOutput = rootRequest.GetRequestStream();

                        status = ValidateCrossing(cross);
                        if (status != HttpStatusCode.Created)
                        {
                            actorResponse.StatusCode = (int)status;
                            actorOutput.Close();
                            actorResponse.Close();
                            return;
                        }
                        // From here on, I might have to write, make sure we only do one of these at a time.
                        // Can't go in UpgradeableLock with a ReadLock, so let go first.
                        m_crossLock.ExitReadLock();
                        m_crossLock.EnterUpgradeableReadLock();
                        try
                        {
                            // First we double check nothing changed while we were waiting for the lock, and we are still valid to cross.
                            status = ValidateCrossing(cross);
                            if (status != HttpStatusCode.Created)
                            {
                                actorResponse.StatusCode = (int)status;
                                actorOutput.Close();
                                actorResponse.Close();
                                return;
                            }

                            // Step 1: Tell root to be ready to receive crossing requests for the object
                            // Root will also do sanity checks (e.g. object was deleted before crossing), so checking actorStatus code
                            // is important.
                            OSDMap DataMap = new OSDMap();
                            DataMap["uuid"] = OSD.FromUUID(cross.uuid);
                            DataMap["pq"] = OSD.FromString(cross.prevQuark);
                            DataMap["cq"] = OSD.FromString(cross.curQuark);
                            DataMap["ts"] = OSD.FromLong(cross.timestamp);

                            string encodedMap = OSDParser.SerializeJsonString(DataMap, true);
                            byte[] rootData = System.Text.Encoding.ASCII.GetBytes(encodedMap);
                            int rootDataLength = rootData.Length;
                            rootOutput.Write(rootData, 0, rootDataLength);
                            rootOutput.Close();

                            // Step 2: Root provides URL for uploading updated properties and for downloading said object.
                            HttpWebResponse response = (HttpWebResponse)rootRequest.GetResponse();
                            if (HttpStatusCode.OK == response.StatusCode)
                            {
                                m_crossLock.EnterWriteLock();
                                try
                                {
                                    m_crossings[cross.uuid] = new CurrentCrossings();
                                    m_crossings[cross.uuid].cross = cross;
                                    m_crossings[cross.uuid].actors.UnionWith(m_quarkSubscriptions[cross.prevQuark].GetAllQuarkSubscribers());
                                    m_crossings[cross.uuid].actors.UnionWith(m_quarkSubscriptions[cross.curQuark].GetAllQuarkSubscribers());
                                    // Remove the starting actor from the list of actors to ACK.
                                    m_crossings[cross.uuid].actors.Remove(cross.actorID);
                                    m_crossings[cross.uuid].rootHandler = root;
                                }
                                finally
                                {
                                    m_crossLock.ExitWriteLock();
                                }
                                Stream respData = response.GetResponseStream();
                                StreamReader rootReader = new StreamReader(respData);
                                url = rootReader.ReadToEnd();
                                m_log.WarnFormat("{0}: Got URL for object request from server: {1}", LogHeader, url);
                                if (url.Length > 0)
                                {
                                    // Step 3: Tell actor about the URL where it may contact the root directly.
                                    // TODO: This can be optimized if we are allowed to remember or calculate the URL. We could respond immediately with the URL
                                    // and the actor would keep trying it until root accepts it. For now, we avoid concurrency. 
                                    actorResponse.StatusCode = (int)HttpStatusCode.Created;
                                    byte[] actorUrlData = System.Text.Encoding.ASCII.GetBytes(url);
                                    int actorUrlDataLength = actorUrlData.Length;
                                    actorOutput.Write(actorUrlData, 0, actorUrlDataLength);
                                    actorOutput.Close();
                                    actorResponse.Close();
                                }
                                else
                                {
                                    m_log.ErrorFormat("{0}: Received empty URL from Root", LogHeader);
                                }
                            }
                            else
                            {
                                m_log.ErrorFormat("{0}: Failed to request crossing from root. Error Code: {1}", LogHeader, response.StatusCode);
                            }
                        }
                        finally
                        {
                            m_crossLock.ExitUpgradeableReadLock();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("{0}: Failed to request crossing from root and forward to actor. Exception: {1}\n{2}", LogHeader, e, e.StackTrace);
            }
            finally
            {
                if (m_crossLock.IsReadLockHeld)
                    m_crossLock.ExitReadLock();
            }
        }

        private void RegisterActor(XMLQuarkSubscription sub, bool isRoot)
        {
            HashSet<string> activeQuarks = SyncQuark.DecodeSyncQuarks(sub.activeQuarks);
            HashSet<string> passiveQuarks = SyncQuark.DecodeSyncQuarks(sub.passiveQuarks);
            QuarkPublisher qp;

            // Save SyncID to XMLQuarkSubscription, if we need the info later
            if (m_actor.ContainsKey(sub.actorID))
                UnRegisterActor(sub.actorID);
            if (isRoot)
            {
                RootInfo root = new RootInfo();
                root.quarkAddress = sub.rootAddress;
                root.syncAddress = sub.syncListenerAddress;
                root.Roots.Add(root);
                m_actor[sub.actorID] = root;
                m_rootActors.Add((RootInfo)m_actor[sub.actorID]);
            }
            else
            {
                if (sub.syncListenerAddress.Length > 0)
                {
                    RelayActor relActor = new RelayActor();
                    relActor.syncAddress = sub.syncListenerAddress;
                    m_actor[sub.actorID] = relActor;
                }
                else
                {
                    Actor actor = new Actor();
                    m_actor[sub.actorID] = actor;
                }
                // Note: Adding all the roots as this actor's root for now.
                foreach (RootInfo root in m_rootActors)
                {
                    m_actor[sub.actorID].Roots.Add(root);
                }
            }
            m_actor[sub.actorID].ActiveQuarks = activeQuarks;
            m_actor[sub.actorID].PassiveQuarks = passiveQuarks;

            m_actor[sub.actorID].XmlSubscription = sub;

            foreach (string quark in activeQuarks)
            {
                if (!QuarkSubscriptions.TryGetValue(quark, out qp))
                {
                    qp = new QuarkPublisher(new SyncQuark(quark));
                    QuarkSubscriptions.Add(quark, qp);
                }
                qp.AddActiveSubscriber(sub.actorID);
                if (isRoot)
                    qp.SetRootActor(sub.actorID);
            }

            foreach (string quark in passiveQuarks)
            {
                if (!QuarkSubscriptions.TryGetValue(quark, out qp))
                {
                    qp = new QuarkPublisher(new SyncQuark(quark));
                    QuarkSubscriptions.Add(quark, qp);
                }
                qp.AddPassiveSubscriber(sub.actorID);
            }
        }

        private void UnRegisterActor(string syncId)
        {
            foreach (QuarkPublisher qp in QuarkSubscriptions.Values)
            {
                qp.RemoveSubscriber(syncId);
            }
        }
    }
}