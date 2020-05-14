<script>
import CardTitle from '@/components/Card-Title'

const httpEndpoint = process.env.VUE_APP_GRAPHQL_HTTP

export default {
  components: {
    CardTitle
  },
  props: {
    fullHeight: {
      required: false,
      type: Boolean,
      default: () => false
    }
  },
  data() {
    return {
      connected: false,
      error: true,
      graphqlUrl: httpEndpoint,
      loading: 0,
      maxRetries: 10,
      retries: 0,
      skip: false
    }
  },
  computed: {
    cardColor() {
      if (this.connected) return 'success'
      if (this.error) return 'Failed'
      return 'grey'
    },
    cardIcon() {
      if (this.connected) return 'signal_cellular_4_bar'
      if (this.error) return 'signal_cellular_off'
      return 'signal_cellular_connected_no_internet_4_bar'
    },
    connecting() {
      return !this.error && !this.connected
    }
  },
  mounted() {},
  apollo: {
    hello: {
      query: require('@/graphql/Dashboard/hello.gql'),
      loadingKey: 'loading',
      result(data) {
        if (!data?.error && 'hello' in data.data && !data.loading) {
          this.connected = true
          this.error = false
        } else {
          this.connected = false
          this.error = false
          if (this.retries < this.maxRetries) {
            this.retries++
          } else {
            this.error = true
            this.skip = true
            setTimeout(() => {
              this.retries = 0
              this.error = false
              this.skip = false
            }, 3000)
          }
        }
        return data.data.hello
      },
      pollInterval: 1000,
      returnPartialData: true,
      fetchPolicy: 'network-only',
      skip() {
        return this.skip
      }
    }
  }
}
</script>

<template>
  <v-card
    class="py-2 position-relative"
    tile
    :style="{
      height: fullHeight ? '100%' : 'auto'
    }"
  >
    <v-system-bar :color="cardColor" :height="5" absolute> </v-system-bar>
    <CardTitle
      title="API Status"
      :icon="cardIcon"
      :icon-color="cardColor"
      :loading="loading > 0"
    >
    </CardTitle>

    <v-list dense class="error-card-content">
      <v-slide-y-transition leave-absolute group>
        <v-list-item key="no-data" color="grey">
          <v-list-item-avatar class="mr-0">
            <v-progress-circular
              v-if="connecting"
              indeterminate
              :size="15"
              :width="2"
              color="primary"
            />
            <v-icon v-else-if="error" class="Failed--text">
              priority_high
            </v-icon>
            <v-icon v-else class="green--text">check</v-icon>
          </v-list-item-avatar>
          <v-list-item-content class="my-0 py-0">
            <div
              class="subtitle-1 font-weight-light"
              style="line-height: 1.25rem;"
            >
              <span v-if="connected">
                Connected
              </span>
              <span v-else-if="error">
                Couldn't connect
              </span>
              <span v-else>
                Connecting
              </span>

              to the API at
              <span class="font-weight-medium">{{ graphqlUrl }}</span>
            </div>

            <div v-if="error" class="caption">
              Retrying in a few seconds...
            </div>
          </v-list-item-content>
        </v-list-item>
      </v-slide-y-transition>
    </v-list>
  </v-card>
</template>

<style lang="scss" scoped>
.api-health-card-content {
  max-height: 254px;
  overflow-y: scroll;
}

.position-relative {
  position: relative;
}

a {
  text-decoration: none !important;
}
</style>
