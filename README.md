# Go Uploader Client

Simple Go client for talking to the upload-service.

## Installation

```bash
go get github.com/alkuinvito/uploader
```

## Creating the client

```go
import (
    "net/http"

    "github.com/alkuinvito/uploader"
)

func main() {
    // Easiest way
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1", // endpoint
        "YOUR_API_KEY",                           // access key
    )

    // Or with custom options
    opts := &uploader.UploadClientOptions{
        AccessKey:  "YOUR_API_KEY",
        ChunkSize:  1024 * 1024, // 1 MB
        Endpoint:   "https://uploads.yourcompany.com/api/v1",
        HTTPClient: &http.Client{},
    }

    client = uploader.NewClient(opts)

    _ = client
}
```

## PutObject
### Upload with chunked blob

```go
import (
    "fmt"
    "log"
    "os"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    // Read file into bytes (example)
    data, err := os.ReadFile("local-file.txt")
    if err != nil {
        log.Fatal(err)
    }

    res, err := client.PutObject("your_bucket", "path/to/file.txt", data, &uploader.UploadOptions{
        ChecksumAlgorithm: uploader.Sha256Sum,
        OnProgress: func(uploaded, total int64) {
            fmt.Printf("uploaded %d / %d bytes\n", uploaded, total)
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("checksum:", res.Checksum, "size:", res.Size)
}
```

## PutObjectForm
### Upload with form file in single request

```go
import (
    "fmt"
    "log"
    "os"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    // Read file into bytes (example)
    data, err := os.ReadFile("local-file.txt")
    if err != nil {
        log.Fatal(err)
    }

    path, err := client.PutObjectForm("your_bucket", "path/to/file.txt", data)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("uploaded path", path)
}
```

## Get object

```go
import (
    "fmt"
    "log"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    data, err := client.GetObject("your_bucket", "path/to/file.txt")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("downloaded bytes:", len(data))
}
```

## Stat object

```go
import (
    "fmt"
    "log"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    object, err := client.StatObject("your_bucket", "path/to/file.txt")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("name:", object.Name, "size:", object.Size)
}
```

## List objects

```go
import (
    "fmt"
    "log"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    objects, err := client.ListObjects("your_bucket", "path/to/", 100)
    if err != nil {
        log.Fatal(err)
    }

    for _, object := range objects {
        fmt.Println("name:", object.Name, "size:", object.Size)
    }
}
```

## Delete object

```go
import (
    "log"

    "github.com/alkuinvito/uploader"
)

func main() {
    client := uploader.NewClientWithDefaults(
        "https://uploads.yourcompany.com/api/v1",
        "YOUR_API_KEY",
    )

    if err := client.DeleteObject("your_bucket", "path/to/file.txt"); err != nil {
        log.Fatal(err)
    }
}
```
