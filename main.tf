//PubSub Topics

resource "google_pubsub_topic" "topic-test" {
  provider = google
  name = "vehiculo"
}


//PubSub Subscriptions

resource "google_pubsub_subscription" "sub-test" {
  topic = "vehiculo" 
  name = "vehiculo_sub"
  depends_on = [
    google_pubsub_topic.topic-test
  ]
}

resource "google_compute_instance" "vm" {
  name         = "example-instance"
  machine_type = "f1-micro"
  zone         = "europe-west1"

  tags = ["foo", "bar"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
    }
  }

  // Local SSD disk
  scratch_disk {
     interface = "SCSI"
  }

  shielded_instance_config {
     enable_integrity_monitoring = true
   }

  network_interface {
     network = "default"
  }

  shielded_instance_config {
     enable_vtpm = true
   }

  metadata = {
     block-project-ssh-keys = true
   }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get update",
      "sudo apt-get install docker.io -y",
      "sudo docker run hello-world"
    ]
  }
}