help "go_build_info" "Build information about the main Go module."
type "go_build_info" gauge
series "go_build_infoÿchecksumÿÿpathÿgithub.com/prometheus/client_golangÿversionÿ(devel)" 3ff0000000000000 - "__name__"="go_build_info" "checksum"="" "path"="github.com/prometheus/client_golang" "version"="(devel)"
st 0
help "go_memstats_alloc_bytes_total" "Total number of bytes allocated, even if freed."
unit "go_memstats_alloc_bytes_total" "bytes"
type "go_memstats_alloc_bytes_total" counter
series "go_memstats_alloc_bytes_total" 4137993000000000 - "__name__"="go_memstats_alloc_bytes_total"
exemplar 4028000000000000 1625851151233 "dummyID"="42"
st 0
help "something_untyped" "Just to test the untyped type."
type "something_untyped" unknown
series "something_untyped" 4045000000000000 1234567 "__name__"="something_untyped"
st 0
help "test_histogram" "Test histogram with many buckets removed to keep it manageable in size."
type "test_histogram" histogram
series "test_histogram_count" 4065e00000000000 1234568 "__name__"="test_histogram_count"
st 0
series "test_histogram_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_histogram_sum"
st 0
series "test_histogram_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_histogram_bucket" "le"="-0.0004899999999999998"
st 0
series "test_histogram_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_histogram_bucket" "le"="-0.0003899999999999998"
exemplar bf398f1d3ed527e5 1625851155146 "dummyID"="59727"
st 0
series "test_histogram_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_histogram_bucket" "le"="-0.0002899999999999998"
exemplar bf330164840e171a - "dummyID"="5617"
st 0
series "test_histogram_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_histogram_bucket" "le"="+Inf"
st 0
help "test_gauge_histogram" "Like test_histogram but as gauge histogram."
type "test_gauge_histogram" gaugehistogram
series "test_gauge_histogram_count" 4065e00000000000 1234568 "__name__"="test_gauge_histogram_count"
st 0
series "test_gauge_histogram_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_gauge_histogram_sum"
st 0
series "test_gauge_histogram_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_gauge_histogram_bucket" "le"="-0.0004899999999999998"
st 0
series "test_gauge_histogram_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_gauge_histogram_bucket" "le"="-0.0003899999999999998"
exemplar bf398f1d3ed527e5 1625851155146 "dummyID"="59727"
st 0
series "test_gauge_histogram_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_gauge_histogram_bucket" "le"="-0.0002899999999999998"
exemplar bf330164840e171a - "dummyID"="5617"
st 0
series "test_gauge_histogram_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_gauge_histogram_bucket" "le"="+Inf"
st 0
help "test_float_histogram" "Test float histogram with many buckets removed to keep it manageable in size."
type "test_float_histogram" histogram
series "test_float_histogram_count" 4065e00000000000 1234568 "__name__"="test_float_histogram_count"
st 0
series "test_float_histogram_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_float_histogram_sum"
st 0
series "test_float_histogram_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_float_histogram_bucket" "le"="-0.0004899999999999998"
st 0
series "test_float_histogram_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_float_histogram_bucket" "le"="-0.0003899999999999998"
exemplar bf398f1d3ed527e5 1625851155146 "dummyID"="59727"
st 0
series "test_float_histogram_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_float_histogram_bucket" "le"="-0.0002899999999999998"
exemplar bf330164840e171a - "dummyID"="5617"
st 0
series "test_float_histogram_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_float_histogram_bucket" "le"="+Inf"
st 0
help "test_gauge_float_histogram" "Like test_float_histogram but as gauge histogram."
type "test_gauge_float_histogram" gaugehistogram
series "test_gauge_float_histogram_count" 4065e00000000000 1234568 "__name__"="test_gauge_float_histogram_count"
st 0
series "test_gauge_float_histogram_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_gauge_float_histogram_sum"
st 0
series "test_gauge_float_histogram_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_gauge_float_histogram_bucket" "le"="-0.0004899999999999998"
st 0
series "test_gauge_float_histogram_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_gauge_float_histogram_bucket" "le"="-0.0003899999999999998"
exemplar bf398f1d3ed527e5 1625851155146 "dummyID"="59727"
st 0
series "test_gauge_float_histogram_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_gauge_float_histogram_bucket" "le"="-0.0002899999999999998"
exemplar bf330164840e171a - "dummyID"="5617"
st 0
series "test_gauge_float_histogram_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_gauge_float_histogram_bucket" "le"="+Inf"
st 0
help "test_histogram2" "Similar histogram as before but now without sparse buckets."
type "test_histogram2" histogram
series "test_histogram2_count" 4065e00000000000 - "__name__"="test_histogram2_count"
st 0
series "test_histogram2_sum" 3f4b21c475e6362a - "__name__"="test_histogram2_sum"
st 0
series "test_histogram2_bucketÿleÿ-0.00048" 4000000000000000 - "__name__"="test_histogram2_bucket" "le"="-0.00048"
st 0
series "test_histogram2_bucketÿleÿ-0.00038" 4010000000000000 - "__name__"="test_histogram2_bucket" "le"="-0.00038"
exemplar bf38e757928e0c9e 1625851153146 "dummyID"="59727"
st 0
series "test_histogram2_bucketÿleÿ1.0" 4030000000000000 - "__name__"="test_histogram2_bucket" "le"="1.0"
exemplar bf3355475a31a4be - "dummyID"="5617"
st 0
series "test_histogram2_bucketÿleÿ+Inf" 4065e00000000000 - "__name__"="test_histogram2_bucket" "le"="+Inf"
st 0
help "test_histogram3" "Similar histogram as before but now with integer buckets."
type "test_histogram3" histogram
series "test_histogram3_count" 4018000000000000 - "__name__"="test_histogram3_count"
st 0
series "test_histogram3_sum" 4049000000000000 - "__name__"="test_histogram3_sum"
st 0
series "test_histogram3_bucketÿleÿ-20.0" 4000000000000000 - "__name__"="test_histogram3_bucket" "le"="-20.0"
st 0
series "test_histogram3_bucketÿleÿ20.0" 4010000000000000 - "__name__"="test_histogram3_bucket" "le"="20.0"
exemplar 402e000000000000 1625851153146 "dummyID"="59727"
st 0
series "test_histogram3_bucketÿleÿ30.0" 4018000000000000 - "__name__"="test_histogram3_bucket" "le"="30.0"
exemplar 4039000000000000 - "dummyID"="5617"
st 0
series "test_histogram3_bucketÿleÿ+Inf" 4018000000000000 - "__name__"="test_histogram3_bucket" "le"="+Inf"
st 0
help "test_histogram_family" "Test histogram metric family with two very simple histograms."
type "test_histogram_family" histogram
series "test_histogram_family_countÿfooÿbar" 4014000000000000 - "__name__"="test_histogram_family_count" "foo"="bar"
st 0
series "test_histogram_family_sumÿfooÿbar" 4028333333333333 - "__name__"="test_histogram_family_sum" "foo"="bar"
st 0
series "test_histogram_family_bucketÿfooÿbarÿleÿ1.1" 4000000000000000 - "__name__"="test_histogram_family_bucket" "foo"="bar" "le"="1.1"
st 0
series "test_histogram_family_bucketÿfooÿbarÿleÿ2.2" 4008000000000000 - "__name__"="test_histogram_family_bucket" "foo"="bar" "le"="2.2"
st 0
series "test_histogram_family_bucketÿfooÿbarÿleÿ+Inf" 4014000000000000 - "__name__"="test_histogram_family_bucket" "foo"="bar" "le"="+Inf"
st 0
series "test_histogram_family_countÿfooÿbaz" 4018000000000000 - "__name__"="test_histogram_family_count" "foo"="baz"
st 0
series "test_histogram_family_sumÿfooÿbaz" 402a333333333333 - "__name__"="test_histogram_family_sum" "foo"="baz"
st 0
series "test_histogram_family_bucketÿfooÿbazÿleÿ1.1" 3ff0000000000000 - "__name__"="test_histogram_family_bucket" "foo"="baz" "le"="1.1"
st 0
series "test_histogram_family_bucketÿfooÿbazÿleÿ2.2" 4014000000000000 - "__name__"="test_histogram_family_bucket" "foo"="baz" "le"="2.2"
st 0
series "test_histogram_family_bucketÿfooÿbazÿleÿ+Inf" 4018000000000000 - "__name__"="test_histogram_family_bucket" "foo"="baz" "le"="+Inf"
st 0
help "test_float_histogram_with_zerothreshold_zero" "Test float histogram with a zero threshold of zero."
type "test_float_histogram_with_zerothreshold_zero" histogram
series "test_float_histogram_with_zerothreshold_zero_count" 4014000000000000 - "__name__"="test_float_histogram_with_zerothreshold_zero_count"
st 0
series "test_float_histogram_with_zerothreshold_zero_sum" 4028333333333333 - "__name__"="test_float_histogram_with_zerothreshold_zero_sum"
st 0
series "test_float_histogram_with_zerothreshold_zero_bucketÿleÿ+Inf" 4014000000000000 - "__name__"="test_float_histogram_with_zerothreshold_zero_bucket" "le"="+Inf"
st 0
help "rpc_durations_seconds" "RPC latency distributions."
type "rpc_durations_seconds" summary
series "rpc_durations_seconds_countÿserviceÿexponential" 4070600000000000 - "__name__"="rpc_durations_seconds_count" "service"="exponential"
st 0
series "rpc_durations_seconds_sumÿserviceÿexponential" 3f30beca61c1da9b - "__name__"="rpc_durations_seconds_sum" "service"="exponential"
st 0
series "rpc_durations_secondsÿquantileÿ0.5ÿserviceÿexponential" 3ea59e4fb26afc41 - "__name__"="rpc_durations_seconds" "quantile"="0.5" "service"="exponential"
st 0
series "rpc_durations_secondsÿquantileÿ0.9ÿserviceÿexponential" 3ec04dcb4485c347 - "__name__"="rpc_durations_seconds" "quantile"="0.9" "service"="exponential"
st 0
series "rpc_durations_secondsÿquantileÿ0.99ÿserviceÿexponential" 3ed0f99b1bacc7ee - "__name__"="rpc_durations_seconds" "quantile"="0.99" "service"="exponential"
st 0
help "without_quantiles" "A summary without quantiles."
type "without_quantiles" summary
series "without_quantiles_count" 4045000000000000 - "__name__"="without_quantiles_count"
st 0
series "without_quantiles_sum" 3ff3be76c8b43958 - "__name__"="without_quantiles_sum"
st 0
help "empty_histogram" "A histogram without observations and with a zero threshold of zero but with a no-op span to identify it as a native histogram."
type "empty_histogram" histogram
series "empty_histogram_count" 0000000000000000 - "__name__"="empty_histogram_count"
st 0
series "empty_histogram_sum" 0000000000000000 - "__name__"="empty_histogram_sum"
st 0
series "empty_histogram_bucketÿleÿ+Inf" 0000000000000000 - "__name__"="empty_histogram_bucket" "le"="+Inf"
st 0
help "test_counter_with_createdtimestamp" "A counter with a created timestamp."
type "test_counter_with_createdtimestamp" counter
series "test_counter_with_createdtimestamp" 4045000000000000 - "__name__"="test_counter_with_createdtimestamp"
st 1625851153146
help "test_summary_with_createdtimestamp" "A summary with a created timestamp."
type "test_summary_with_createdtimestamp" summary
series "test_summary_with_createdtimestamp_count" 4045000000000000 - "__name__"="test_summary_with_createdtimestamp_count"
st 1625851153146
series "test_summary_with_createdtimestamp_sum" 3ff3be76c8b43958 - "__name__"="test_summary_with_createdtimestamp_sum"
st 1625851153146
help "test_histogram_with_createdtimestamp" "A histogram with a created timestamp."
type "test_histogram_with_createdtimestamp" histogram
series "test_histogram_with_createdtimestamp_count" 0000000000000000 - "__name__"="test_histogram_with_createdtimestamp_count"
st 1625851153146
series "test_histogram_with_createdtimestamp_sum" 0000000000000000 - "__name__"="test_histogram_with_createdtimestamp_sum"
st 1625851153146
series "test_histogram_with_createdtimestamp_bucketÿleÿ+Inf" 0000000000000000 - "__name__"="test_histogram_with_createdtimestamp_bucket" "le"="+Inf"
st 1625851153146
help "test_gaugehistogram_with_createdtimestamp" "A gauge histogram with a created timestamp."
type "test_gaugehistogram_with_createdtimestamp" gaugehistogram
series "test_gaugehistogram_with_createdtimestamp_count" 0000000000000000 - "__name__"="test_gaugehistogram_with_createdtimestamp_count"
st 1625851153146
series "test_gaugehistogram_with_createdtimestamp_sum" 0000000000000000 - "__name__"="test_gaugehistogram_with_createdtimestamp_sum"
st 1625851153146
series "test_gaugehistogram_with_createdtimestamp_bucketÿleÿ+Inf" 0000000000000000 - "__name__"="test_gaugehistogram_with_createdtimestamp_bucket" "le"="+Inf"
st 1625851153146
help "test_histogram_with_native_histogram_exemplars" "A histogram with native histogram exemplars."
type "test_histogram_with_native_histogram_exemplars" histogram
series "test_histogram_with_native_histogram_exemplars_count" 4065e00000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_count"
st 0
series "test_histogram_with_native_histogram_exemplars_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_sum"
st 0
series "test_histogram_with_native_histogram_exemplars_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_bucket" "le"="-0.0004899999999999998"
st 0
series "test_histogram_with_native_histogram_exemplars_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_bucket" "le"="-0.0003899999999999998"
exemplar bf398f1d3ed527e5 1625851155146 "dummyID"="59727"
st 0
series "test_histogram_with_native_histogram_exemplars_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_bucket" "le"="-0.0002899999999999998"
exemplar bf330164840e171a - "dummyID"="5617"
st 0
series "test_histogram_with_native_histogram_exemplars_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars_bucket" "le"="+Inf"
st 0
help "test_histogram_with_native_histogram_exemplars2" "Another histogram with native histogram exemplars."
type "test_histogram_with_native_histogram_exemplars2" histogram
series "test_histogram_with_native_histogram_exemplars2_count" 4065e00000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_count"
st 0
series "test_histogram_with_native_histogram_exemplars2_sum" 3f4b22279eab7b5d 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_sum"
st 0
series "test_histogram_with_native_histogram_exemplars2_bucketÿleÿ-0.0004899999999999998" 4000000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_bucket" "le"="-0.0004899999999999998"
st 0
series "test_histogram_with_native_histogram_exemplars2_bucketÿleÿ-0.0003899999999999998" 4010000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_bucket" "le"="-0.0003899999999999998"
st 0
series "test_histogram_with_native_histogram_exemplars2_bucketÿleÿ-0.0002899999999999998" 4030000000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_bucket" "le"="-0.0002899999999999998"
st 0
series "test_histogram_with_native_histogram_exemplars2_bucketÿleÿ+Inf" 4065e00000000000 1234568 "__name__"="test_histogram_with_native_histogram_exemplars2_bucket" "le"="+Inf"
st 0
