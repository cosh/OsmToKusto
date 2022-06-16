using OsmSharp;
using OsmSharp.Tags;
using OsmToKusto.Ingestion;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OsmToKusto
{
    public static class Helper
    {
        public static char csvSeparator = '|';

        public static string CleanString(string osmId, char csvSeparater)
        {
            return Regex.Replace(osmId.Replace(csvSeparater, '#'), @"\r\n?|\n", "");
        }

        public static string GetMembersArray(RelationMember[] members)
        {
            if (members != null && members.Length > 0)
            {
                return "[" + String.Join(',', members.Select(_ => $"[\"{_.Id}\", \"{_.Role}\", \"{_.Type}\"]")) + "]";
            }

            return String.Empty;
        }

        public static string GetNodesArray(long[] nodes)
        {
            if (nodes != null && nodes.Length > 0)
            {
                return "[" + String.Join(',', nodes.Select(_ => $"[\"{_}\", \"\", \"Node\"]")) + "]";
            }

            return String.Empty;
        }

        public static string CreateTagString(TagsCollectionBase tags)
        {
            if (tags != null && tags.Count > 0)
            {
                return "[" + String.Join(',', tags.Select(aTag => $"[\"{aTag.Key}\", \"{aTag.Value}\"]")) + "]";
            }

            return String.Empty;
        }

        public static void IngestToKusto(string databaseName, string table,
            StringBuilder sb, String mappingName, IngestionManager iManager)
        {
            var job = new IngestionJob();
            job.Table = table;
            job.MappingName = mappingName;
            job.DatabaseName = databaseName;

            string tempFile = Path.GetRandomFileName();
            File.WriteAllText(tempFile, sb.ToString());

            job.ToBeIngested = tempFile;

            iManager.Enqueue(job);
        }
    }
}
