using System;
using System.Net;
using log4net;
using System.Reflection;
using System.IO;
using log4net.Config;
using System.Xml.Serialization;
using System.Collections.Generic;

namespace QuarkService
{
    public class QuarkServiceListener
    {
        private static ILog m_log;
        private bool m_running = false;
        private HttpListener m_listener;
        private List<RegionSyncListenerInfo> m_rootActors = new List<RegionSyncListenerInfo>();
        private Dictionary<string, XMLQuarkSubscription> m_xmlSubscriptions = new Dictionary<string, XMLQuarkSubscription>();

        private Dictionary<string, QuarkPublisher> m_quarkSubscriptions = new Dictionary<string, QuarkPublisher>();
        public Dictionary<string, QuarkPublisher> QuarkSubscriptions
        {
            get { return m_quarkSubscriptions; }
        }

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
                // TODO: Get root actors information and provide a method that translates quark-to-root parent.
                RegionSyncListenerInfo root = new RegionSyncListenerInfo("127.0.0.1", 8080);
                m_rootActors.Add(root);
            // </DELETEME>
            m_listener.Prefixes.Add("http://" + localIP + ":8080/quark/");
            m_listener.Start();
            m_running = true;
            m_log.Info("[QUARKSERVICE]: Listening on " + localIP + ":8080");
            while (m_running)
            {
                try
                {
                    // Note: The GetContext method blocks while waiting for a request. 
                    HttpListenerContext context = m_listener.GetContext();
                    HttpListenerRequest request = context.Request;
                    if (request.RawUrl.Contains("subscribe"))
                    {

                        StreamReader reader = new StreamReader(request.InputStream);
                        XmlSerializer deserializer = new XmlSerializer(typeof(XMLQuarkSubscription));
                        XMLQuarkSubscription quarkSub = (XMLQuarkSubscription)deserializer.Deserialize(reader);
                        RegisterActor(quarkSub);

                        // Obtain a response object.

                        HttpListenerResponse response = context.Response;
                        Stream output = response.OutputStream;
                        // Construct a response. 
                        System.Xml.Serialization.XmlSerializer x = new System.Xml.Serialization.XmlSerializer(m_rootActors.GetType());
                        x.Serialize(output, m_rootActors);
                        output.Close();
                    }
                    else if (request.RawUrl.Contains("cross"))
                    {
                        m_log.Info("[QUARKSERVICE]: Not Yet implemented");
                    }
                }
                catch (Exception e)
                {
                    m_log.Error("[QUARKSERVICE]: HTTP request could not be handled.", e);
                }
            }
        }

        private void RegisterActor(XMLQuarkSubscription sub)
        {
            HashSet<string> activeQuarks = SyncQuark.DecodeSyncQuarks(sub.activeQuarks);
            HashSet<string> passiveQuarks = SyncQuark.DecodeSyncQuarks(sub.passiveQuarks);
            QuarkPublisher qp;

            // Save SyncID to XMLQuarkSubscription, if we need the info later
            m_xmlSubscriptions.Add(sub.syncID, sub);

            foreach (string quark in activeQuarks)
            {
                if (!QuarkSubscriptions.TryGetValue(quark, out qp))
                {
                    qp = new QuarkPublisher(new SyncQuark(quark));
                    QuarkSubscriptions.Add(quark, qp);
                }
                qp.AddActiveSubscriber(sub.syncID);
            }

            foreach (string quark in passiveQuarks)
            {
                if (!QuarkSubscriptions.TryGetValue(quark, out qp))
                {
                    qp = new QuarkPublisher(new SyncQuark(quark));
                    QuarkSubscriptions.Add(quark, qp);
                }
                qp.AddPassiveSubscriber(sub.syncID);
            }
        }
    }
}