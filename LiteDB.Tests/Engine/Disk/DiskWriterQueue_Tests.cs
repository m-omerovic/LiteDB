using System;
using LiteDB.Engine;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace LiteDB.Tests.Engine.Disk
{
    public class DiskWriterQueue_Tests
    {
        public class TestStream : Stream
        {
            public override void Flush()
            {
                // Do nothing.
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Interlocked.Increment(ref _writeCounter);
            }

            public override bool CanRead { get; }
            public override bool CanSeek { get; }
            public override bool CanWrite { get; }
            public override long Length { get; }
            public override long Position { get; set; }
            public int WriteCounter => _writeCounter;
            private int _writeCounter;
        }

        public class CustomConsoleOutputWriter : StringWriter
        {
            private readonly StreamWriter _streamWriter;
            private readonly Timer _timer;

            public CustomConsoleOutputWriter()
            {
                const string fileName = $"{nameof(DiskWriterQueue_Tests)}.log";
                if (File.Exists(fileName)) File.Delete(fileName);
                _streamWriter = File.AppendText(fileName);
//                _streamWriter.AutoFlush = true;
                _timer = new Timer(_ => { _streamWriter.Flush(); }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
            }

            public override void WriteLine(string m)
            {
                _streamWriter.WriteLine($"{DateTime.Now:HH:mm:ss.fff} - {Environment.CurrentManagedThreadId} - {m}");
            }

            public override ValueTask DisposeAsync()
            {
                _timer.Dispose();
                _streamWriter.Flush();
                _streamWriter.Dispose();
                return base.DisposeAsync();
            }
        }

        [Fact]
        public void Test()
        {
            using var writer = new CustomConsoleOutputWriter();
            Console.SetOut(writer);

            const int iterations = 4;
            const int runsPerThread = 10000;
            const int pagesPerThread = 1;

            var pageBuffer = new PageBuffer(new byte[Constants.PAGE_SIZE], 0, 1)
            {
                Origin = FileOrigin.Log, 
                ShareCounter = iterations * runsPerThread * pagesPerThread,
                Position = 0
            };
            //const string fileName = nameof(DiskWriterQueue_Tests);
            //if (File.Exists(fileName)) File.Delete(fileName);
            //using var testStream = File.Create(fileName);
            using var testStream = new TestStream();
            using var subject = new DiskWriterQueue(testStream);
            Console.WriteLine("Creating pages.");
            Parallel.For(0, iterations, (_) =>
            {
                for (var i = 0; i < runsPerThread; i++)
                {
                    for (var j = 0; j < pagesPerThread; j++)
                    {
                        subject.EnqueuePage(pageBuffer);
                    }

                    subject.Run();
                    Thread.Sleep(RandomNumberGenerator.GetInt32(0, 100));
                    //subject.Wait();
                }
            });

            Console.WriteLine("Waiting for processing to finish.");
            subject.Wait();

            Assert.Equal(0, subject.Length);
            Assert.Equal(iterations * runsPerThread * pagesPerThread, testStream.WriteCounter);
        }
    }
}